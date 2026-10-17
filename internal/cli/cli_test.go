package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/cli"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

// Tests that use it don't run in parallel since it sets GATEREACH_CONFIG
func useConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "conns.toml")
	t.Setenv("GATEREACH_CONFIG", p)
	if content != "" {
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	}
	return p
}

func run(args ...string) (int, string, string) {
	var out, errw bytes.Buffer
	code := cli.Run(args, &out, &errw)
	return code, out.String(), errw.String()
}

const existing = "[conns.db]\nclient = \"mysql\"\nhost = \"127.0.0.1\"\nport = 22912\nuser = \"viewer\"\n"

func TestRun(t *testing.T) {
	type want struct {
		code     int
		out, err string
	}
	tcs := []struct {
		name    string
		content string
		args    []string
		want    want
	}{
		{"version", "", []string{"--version"}, want{code: 0, out: version.Version + "\n"}},
		{"version word", "", []string{"version"}, want{code: 0, out: version.Version + "\n"}},
		{"usage hides serve", "", nil, want{code: 2, err: "gatereach q"}},
		{"unknown command", "", []string{"nope"}, want{code: 2, err: "unknown command"}},
		{"broken config", "[conns.a\n", []string{"ls"}, want{code: 1, err: "couldn't read the config file"}},
		{"unknown tunnel kind to discover", "", []string{"discover", "ssh"}, want{code: 1, err: "unsupported tunnel"}},
		{"close an unknown alias", "", []string{"close", "nope"}, want{code: 1, err: "unknown alias"}},
		{"close all with nothing open", existing, []string{"close", "all"}, want{code: 0}},
		{"close shows escapes in an alias", "[conns.\"e\\u001b\"]\nclient = \"redis\"\nport = 1\n",
			[]string{"close", "e\x1b"}, want{code: 0, out: "e\\u001B already closed\n"}},
		{"flag help", "", []string{"q", "-h"}, want{code: 2}},
		{"bad flag value", "", []string{"q", "r", "GET k", "--limit", "abc"}, want{code: 2, err: "invalid value"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, tc.content)
			code, out, errw := run(tc.args...)
			assert.Equal(t, tc.want.code, code, errw)
			assert.Contains(t, out, tc.want.out)
			assert.Contains(t, errw, tc.want.err)
			if tc.want.err != "" {
				assert.Equal(t, 1, strings.Count(errw, tc.want.err), "printed once")
			}
			assert.NotContains(t, errw, "gatereach serve")
		})
	}
}
