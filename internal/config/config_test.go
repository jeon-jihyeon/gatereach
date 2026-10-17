package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

// Tests that use it don't run in parallel since it sets GATEREACH_CONFIG
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
		err  string
	}
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"a target and user always get the same port", args{"db-a", "viewer", nil}, want{port: 22912}},
		{"another target and user get their own port", args{"orders-replica", "reader", nil}, want{port: 30159}},
		{"skips a used port", args{"db-a", "viewer", map[int]bool{22912: true}}, want{port: 22913}},
		{"wraps to the start of the range", args{"db-a", "viewer", rangeFrom(22912, all)}, want{port: 20000}},
		{"range exhausted", args{"a", "b", all}, want{err: "no fixed port left between 20000 and 31999"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := config.StablePort(tc.args.target, tc.args.user, tc.args.used)
			assert.Equal(t, tc.want, want{port: got, err: errText(err)})
		})
	}
}

// The ports of used from start to the end of the range
func rangeFrom(start int, used map[int]bool) map[int]bool {
	out := map[int]bool{}
	for p := range used {
		if p >= start {
			out[p] = true
		}
	}
	return out
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
				assert.ErrorContains(t, err, "couldn't read the config file")
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
		"b": {Host: "192.0.2.1", Port: 2},
		"c": {Profile: "p"},
	}}
	tcs := []struct {
		name string
		host string
		want map[int]bool
	}{
		{"ports on the host", "127.0.0.1", map[int]bool{1: true}},
		{"another host", "192.0.2.1", map[int]bool{2: true}},
		{"no connection on the host", "192.0.2.9", map[int]bool{}},
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
		{"workgroup set", "workgroup", config.Conn{Workgroup: "primary"}, true},
		{"output location unset", "output_location", config.Conn{Workgroup: "primary"}, false},
		{"catalog set", "catalog", config.Conn{Catalog: "AwsDataCatalog"}, true},
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
	p := useConfig(t, "# a hand-written comment\n")
	c := config.Conn{
		Client: "mysql",
		Host:   config.DefaultHost,
		Port:   21000,
		User:   "viewer",
		Secret: "env:GATEREACH_PW",
		TLS:    true,
		Write:  true,
		Tunnel: &config.Tunnel{Type: "tsh", Target: "db \"a\" \\ café \x7f"},
	}
	got, err := config.Append("x.y", c)
	require.NoError(t, err)
	assert.Equal(t, p, got)
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.Contains(t, string(b), "# a hand-written comment")
	assert.NotContains(t, string(b), "\x7f")
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, c, cfg.Conns["x.y"])
	fi, err := os.Stat(p)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

func TestAppend_Errors(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	tcs := []struct {
		name   string
		config string
		alias  string
		err    string
	}{
		{"a value the config can't hold", filepath.Join(t.TempDir(), "conns.toml"), "\xff",
			"a character the config can't hold"},
		{"a file in place of the config folder", filepath.Join(blocker, "conns.toml"), "x", "not a directory"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GATEREACH_CONFIG", tc.config)
			_, err := config.Append(tc.alias, config.Conn{Client: "redis"})
			assert.ErrorContains(t, err, tc.err)
			assert.NoFileExists(t, tc.config)
		})
	}
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
		{"keeps non-ASCII", "café", "café"},
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
		err   string
	}{
		{"control characters are escaped", "x", config.Conn{Client: "redis", User: "a\x00b\nc"}, ""},
		{"athena keys round trip", "x", config.Conn{Client: "athena", Region: "us-east-1", Workgroup: "primary",
			OutputLocation: "s3://example-bucket/results/", Catalog: "AwsDataCatalog", Database: "acme"}, ""},
		{"invalid UTF-8 is refused", "\xff", config.Conn{Client: "redis"},
			"a character the config can't hold\ncheck the values for invalid UTF-8"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, err := config.Render(tc.alias, tc.conn)
			assert.Equal(t, tc.err, errText(err))
			assert.NotContains(t, text, "\x00")
		})
	}
}
