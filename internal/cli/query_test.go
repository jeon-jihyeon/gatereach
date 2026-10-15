package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery_에러(t *testing.T) {
	file := filepath.Join(t.TempDir(), "q.sql")
	require.NoError(t, os.WriteFile(file, []byte("FLUSHALL"), 0o600))
	// 쿼리와 같은 이름의 파일이 있어도 위치 인자는 쿼리 텍스트
	cwd := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cwd, "DEL k"), []byte("GET k"), 0o600))
	t.Chdir(cwd)
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "x"), nil, 0o600))
	type want struct {
		code int
		err  string
	}
	tcs := []struct {
		name string
		args []string
		want want
	}{
		{"unknown alias", []string{"q", "nope", "GET k"}, want{1, "알 수 없는 별칭: nope"}},
		{"write blocked before the tunnel opens", []string{"q", "r", "FLUSHALL"}, want{1, "읽기 전용 모드에서 막힌 명령: FLUSHALL"}},
		{"query from --file", []string{"q", "r", "--file", file}, want{1, "읽기 전용 모드에서 막힌 명령: FLUSHALL"}},
		{"query after -- may start with a dash", []string{"q", "r", "--", "-x"}, want{1, "읽기 전용 모드에서 막힌 명령: -x"}},
		{"a file named like the query is not read", []string{"q", "r", "DEL k"}, want{1, "읽기 전용 모드에서 막힌 명령: DEL"}},
		{"limit must be at least 1", []string{"q", "r", "GET k", "--limit", "0"}, want{1, "--limit은 1 이상"}},
		{"a non-empty --out is refused before the tunnel opens", []string{"q", "r", "GET k", "--out", full}, want{1, "비어 있지 않은 폴더"}},
		{"interval must be positive", []string{"q", "r", "GET k", "--interval", "0"}, want{1, "--interval은 0보다 커야 함"}},
		{"unknown format", []string{"q", "r", "GET k", "--out", "x", "--format", "parquet"}, want{1, "지원하지 않는 형식"}},
		{"missing query", []string{"q", "r"}, want{1, "사용법"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, "[conns.r]\nclient = \"redis\"\nport = 1\n\n[conns.r.tunnel]\ntype = \"tsh\"\ntarget = \"x\"\n")
			code, _, errw := run(tc.args...)
			assert.Equal(t, tc.want.code, code)
			assert.Contains(t, errw, tc.want.err)
		})
	}
}
