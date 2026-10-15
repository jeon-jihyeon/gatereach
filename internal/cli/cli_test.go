package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/cli"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

// GATEREACH_CONFIG를 바꾸므로 병렬 실행하지 않음
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
		{"usage hides serve", "", nil, want{code: 2, err: "gatereach q"}},
		{"unknown command", "", []string{"nope"}, want{code: 2, err: "알 수 없는 명령"}},
		{"broken config", "[conns.a\n", []string{"ls"}, want{code: 1, err: "설정 파일 읽기 실패"}},
		{"unknown tunnel kind to discover", "", []string{"discover", "ssh"}, want{code: 1, err: "지원하지 않는 터널"}},
		{"close an unknown alias", "", []string{"close", "nope"}, want{code: 1, err: "알 수 없는 별칭"}},
		{"close all with nothing open", existing, []string{"close", "all"}, want{code: 0}},
		{"flag help", "", []string{"q", "-h"}, want{code: 2}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, tc.content)
			code, out, errw := run(tc.args...)
			assert.Equal(t, tc.want.code, code, errw)
			assert.Contains(t, out, tc.want.out)
			assert.Contains(t, errw, tc.want.err)
			assert.NotContains(t, errw, "gatereach serve")
		})
	}
}
