package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

// GATEREACH_CONFIG를 바꾸므로 병렬 실행하지 않음
func useConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gatereach", "conns.toml")
	t.Setenv("GATEREACH_CONFIG", p)
	if content != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o700))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	}
	return p
}

func TestStablePort(t *testing.T) {
	t.Parallel()
	all := map[int]bool{}
	for p := config.PortBase; p < config.PortBase+config.PortSpan; p++ {
		all[p] = true
	}
	type args struct {
		target, user string
		used         map[int]bool
	}
	type want struct {
		port int
		err  bool
	}
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"a target and user always get the same port", args{"db-a", "viewer", nil}, want{port: 22912}},
		{"another target and user get their own port", args{"orders-replica", "reader", nil}, want{port: 30159}},
		{"skips a used port", args{"db-a", "viewer", map[int]bool{22912: true}}, want{port: 22913}},
		{"range exhausted", args{"a", "b", all}, want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := config.StablePort(tc.args.target, tc.args.user, tc.args.used)
			if tc.want.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.port, got)
		})
	}
}

func TestLoad(t *testing.T) {
	type want struct {
		aliases []string
		hosts   map[string]string
		err     bool
	}
	tcs := []struct {
		name    string
		content string
		want    want
	}{
		{"missing file is empty", "", want{}},
		{"file order and default host only with a port",
			"[conns.b]\nclient = \"mysql\"\nport = 3306\n\n[conns.a]\nclient = \"dynamodb\"\nprofile = \"p\"\n",
			want{aliases: []string{"b", "a"}, hosts: map[string]string{"b": config.DefaultHost, "a": ""}}},
		{"broken TOML", "[conns.a\n", want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, tc.content)
			cfg, err := config.Load()
			if tc.want.err {
				assert.ErrorContains(t, err, "설정 파일 읽기 실패")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.aliases, cfg.Aliases)
			for alias, host := range tc.want.hosts {
				assert.Equal(t, host, cfg.Conns[alias].Host, alias)
			}
		})
	}
}

func TestTool(t *testing.T) {
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	cfg := &config.Config{Tunnels: map[string]config.Tool{"tsh": {Bin: "~/bin/tsh", Home: "~/tsh"}}}
	tcs := []struct {
		name string
		kind string
		want config.Tool
	}{
		{"expands home", "tsh", config.Tool{Bin: filepath.Join(home, "bin/tsh"), Home: filepath.Join(home, "tsh")}},
		{"falls back to the bare name", "other", config.Tool{Bin: "other"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, cfg.Tool(tc.kind, tc.kind))
		})
	}
}

func TestUsedPorts(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Conns: map[string]config.Conn{
		"a": {Host: "127.0.0.1", Port: 1},
		"b": {Host: "10.0.0.1", Port: 2},
		"c": {Profile: "p"},
	}}
	tcs := []struct {
		name string
		host string
		want map[int]bool
	}{
		{"ports on the host", "127.0.0.1", map[int]bool{1: true}},
		{"another host", "10.0.0.1", map[int]bool{2: true}},
		{"no connection on the host", "10.0.0.9", map[int]bool{}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, cfg.UsedPorts(tc.host))
		})
	}
}

func TestHas(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		key  string
		conn config.Conn
		want bool
	}{
		{"port set", "port", config.Conn{Port: 1}, true},
		{"port zero", "port", config.Conn{}, false},
		{"profile set", "profile", config.Conn{Profile: "p"}, true},
		{"tunnel set", "tunnel", config.Conn{Tunnel: &config.Tunnel{}}, true},
		{"unknown key", "unknown", config.Conn{User: "u"}, false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.conn.Has(tc.key))
		})
	}
}

func TestAppend(t *testing.T) {
	p := useConfig(t, "# 손으로 쓴 주석\n")
	c := config.Conn{Client: "mysql", Host: config.DefaultHost, Port: 21000, User: "viewer", Secret: "env:PW", TLS: true,
		Tunnel: &config.Tunnel{Type: "tsh", Target: "db \"a\" \\ 한글 \x7f"}}
	got, err := config.Append("x.y", c)
	require.NoError(t, err)
	assert.Equal(t, p, got)
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.Contains(t, string(b), "# 손으로 쓴 주석")
	assert.NotContains(t, string(b), "\x7f")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, c, cfg.Conns["x.y"])
	fi, err := os.Stat(p)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

func TestFileName(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name  string
		alias string
		want  string
	}{
		{"keeps dots and dashes", "a/b.c-d", "a_b.c-d"},
		{"a run of others becomes one underscore", "a / b", "a_b"},
		{"keeps non-ASCII", "한글", "한글"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, config.FileName(tc.alias))
		})
	}
}

func TestLogPath(t *testing.T) {
	useConfig(t, "")
	tcs := []struct {
		name   string
		alias  string
		suffix string
		want   string
	}{
		{"tunnel log", "a b", ".log", "a_b-806c5cd3.log"},
		{"an alias with the same file name gets another log", "a_b", ".log", "a_b-28cb39ea.log"},
		{"session log", "db", ".session.log", "db-e3f4bc28.session.log"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, filepath.Join(config.Dir(), "logs", tc.want), config.LogPath(tc.alias, tc.suffix))
		})
	}
}

func TestRender(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name  string
		alias string
		conn  config.Conn
		err   bool
	}{
		{"control characters are escaped", "x", config.Conn{Client: "redis", User: "a\x00b\nc"}, false},
		{"invalid UTF-8 is refused", "\xff", config.Conn{Client: "redis"}, true},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, err := config.Render(tc.alias, tc.conn)
			if tc.err {
				assert.ErrorContains(t, err, "설정 값으로 쓸 수 없는 문자")
				return
			}
			require.NoError(t, err)
			assert.NotContains(t, text, "\x00")
		})
	}
}
