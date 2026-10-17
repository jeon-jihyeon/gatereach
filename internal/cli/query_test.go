package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery_Errors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "q.sql")
	require.NoError(t, os.WriteFile(file, []byte("FLUSHALL"), 0o600))
	// The positional argument stays query text even when a file has the same name
	cwd := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cwd, "DEL k"), []byte("GET k"), 0o600))
	t.Chdir(cwd)
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "x"), nil, 0o600))
	// Exactly 1 MiB is still read
	limit := filepath.Join(t.TempDir(), "limit.txt")
	require.NoError(t, os.WriteFile(limit, append([]byte("FLUSHALL"), bytes.Repeat([]byte(" "), 1<<20-8)...), 0o600))
	large := filepath.Join(t.TempDir(), "large.txt")
	require.NoError(t, os.WriteFile(large, bytes.Repeat([]byte(" "), 1<<20+1), 0o600))
	// Opening a FIFO with no writer would block without O_NONBLOCK
	fifo := filepath.Join(t.TempDir(), "fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	type want struct {
		code int
		err  string
	}
	tcs := []struct {
		name string
		args []string
		want want
	}{
		{"unknown alias", []string{"q", "nope", "GET k"}, want{1, "unknown alias: nope"}},
		{"write blocked before the tunnel opens", []string{"q", "r", "FLUSHALL"},
			want{1, "command blocked in read-only mode: FLUSHALL"}},
		{"query from --file", []string{"q", "r", "--file", file}, want{1, "command blocked in read-only mode: FLUSHALL"}},
		{"query after -- may start with a dash", []string{"q", "r", "--", "-x"},
			want{1, "command blocked in read-only mode: -x"}},
		{"a file named like the query is not read", []string{"q", "r", "DEL k"},
			want{1, "command blocked in read-only mode: DEL"}},
		{"limit must be at least 1", []string{"q", "r", "GET k", "--limit", "0"}, want{1, "--limit must be at least 1"}},
		{"a non-empty --out is refused before the tunnel opens", []string{"q", "r", "GET k", "--out", full},
			want{1, "folder not empty"}},
		{"interval must be positive", []string{"q", "r", "GET k", "--interval", "0"},
			want{1, "--interval must be at least 1 second"}},
		{"interval under a second is refused", []string{"q", "r", "GET k", "--interval", "0.5"},
			want{1, "--interval must be at least 1 second"}},
		{"unknown format", []string{"q", "r", "GET k", "--out", "x", "--format", "parquet"}, want{1, "unsupported format"}},
		{"missing query", []string{"q", "r"}, want{1, "usage"}},
		{"athena blocks writes before the session starts", []string{"q", "a", "DROP TABLE t"},
			want{1, "read-only mode allows only SELECT"}},
		{"a bad --max is refused before the tunnel opens", []string{"q", "r", "GET k", "--max", "1.5"},
			want{1, "redis --max takes reply elements, such as 1000"}},
		{"athena --max takes bytes", []string{"q", "a", "SELECT 1", "--max", "10XB"},
			want{1, "athena --max takes bytes scanned, such as 10GB"}},
		{"--max is checked before --out", []string{"q", "r", "GET k", "--max", "0", "--out", full},
			want{1, "redis --max takes reply elements, such as 1000"}},
		{"timeout below 0 is refused", []string{"q", "r", "GET k", "--timeout", "-1"},
			want{1, "--timeout must be 0 or above"}},
		{"timeout NaN is refused", []string{"q", "r", "GET k", "--timeout", "NaN"}, want{1, "--timeout must be 0 or above"}},
		{"timeout past what a duration holds is refused", []string{"q", "r", "GET k", "--timeout", "1e300"},
			want{1, "--timeout must be under 9223372036 seconds"}},
		{"timeout at the bound with a fraction is refused", []string{"q", "r", "GET k", "--timeout", "9223372036.5"},
			want{1, "--timeout must be under 9223372036 seconds"}},
		{"timeout that truncates to zero is refused", []string{"q", "r", "GET k", "--timeout", "1e-12"},
			want{1, "--timeout must be at least one nanosecond"}},
		{"interval NaN is refused", []string{"q", "r", "GET k", "--interval", "NaN"},
			want{1, "--interval must be at least 1 second"}},
		{"--write needs write = true on the alias", []string{"q", "r", "SET k v", "--write"},
			want{1, "r doesn't allow --write\nset write = true for it in "}},
		{"an alias with write = true passes --write on", []string{"q", "a", "DROP TABLE t", "--write", "--max", "10XB"},
			want{1, "athena --max takes bytes scanned"}},
		{"a folder as --file is refused", []string{"q", "r", "--file", cwd}, want{1, "--file must be a regular file"}},
		{"a FIFO as --file is refused without blocking", []string{"q", "r", "--file", fifo},
			want{1, "--file must be a regular file"}},
		{"a --file of exactly 1 MiB is read", []string{"q", "r", "--file", limit},
			want{1, "command blocked in read-only mode: FLUSHALL"}},
		{"a --file over 1 MiB is refused", []string{"q", "r", "--file", large}, want{1, "--file is larger than 1 MiB"}},
		{"a missing --file fails", []string{"q", "r", "--file", filepath.Join(cwd, "missing.sql")},
			want{1, "couldn't read --file"}},
		{"an unknown format lists the formats", []string{"q", "r", "GET k", "--format", "parquet"},
			want{1, "unsupported format: parquet\navailable: arrow csv"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, "[conns.r]\nclient = \"redis\"\nport = 1\n\n[conns.r.tunnel]\ntype = \"tsh\"\ntarget = \"x\"\n\n"+
				"[conns.a]\nclient = \"athena\"\nworkgroup = \"primary\"\nwrite = true\n")
			code, _, errw := run(tc.args...)
			assert.Equal(t, tc.want.code, code)
			assert.Contains(t, errw, tc.want.err)
		})
	}
}
