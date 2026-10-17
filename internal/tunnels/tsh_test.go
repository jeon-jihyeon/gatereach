package tunnels

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

func TestTshPids(t *testing.T) {
	t.Parallel()
	c := config.Conn{Port: 21000, User: "viewer", Tunnel: &config.Tunnel{Type: "tsh", Target: "db"}}
	type want struct {
		pids []int
		err  string
	}
	tcs := []struct {
		name string
		out  string
		err  error
		want want
	}{
		{"matches only the exact command line", " 11 /opt/tsh proxy db --tunnel --port 21000 --db-user=viewer -- db\n" +
			" 12 /opt/tsh proxy db --tunnel --port 21000 --db-user=viewer -- db --debug\n" +
			" 14 /opt/tsh proxy db --tunnel --port 21000 --db-user viewer db\n" +
			" 13 Teleport Connect\n", nil, want{pids: []int{11}}},
		{"ps fails", "", errors.New("no ps"), want{err: "process list failed: no ps"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k := tsh{tool: config.Tool{Bin: "/opt/tsh"}, ps: func() ([]byte, error) { return []byte(tc.out), tc.err }}
			got, err := k.Pids(c)
			assert.Equal(t, tc.want.pids, got)
			assert.Equal(t, tc.want.err, errText(err))
		})
	}
}

func TestTshDiscover(t *testing.T) {
	t.Parallel()
	type want struct {
		targets []Target
		err     string
	}
	tcs := []struct {
		name string
		// Body of a fake tsh, empty for a binary that doesn't exist
		script string
		want   want
	}{
		{"lists the databases", `echo '[{"metadata":{"name":"db-1"},"spec":{"protocol":"redis"}}]'`,
			want{targets: []Target{{Target: "db-1", Name: "db-1", Client: "redis"}}}},
		{"a failed tsh points at the login", "echo 'not logged in' >&2; exit 1",
			want{err: "tsh db ls failed: check the tsh login\nnot logged in"}},
		{"a missing tsh gives the cause", "", want{err: "tsh db ls couldn't run"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bin := filepath.Join(t.TempDir(), "tsh")
			if tc.script != "" {
				require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\n"+tc.script+"\n"), 0o700))
			}
			got, err := tsh{tool: config.Tool{Bin: bin}}.Discover()
			assert.Equal(t, tc.want.targets, got)
			assert.Equal(t, tc.want.err == "", err == nil, "error: %v", err)
			assert.Contains(t, errText(err), tc.want.err)
		})
	}
}

func TestTshCommand(t *testing.T) {
	t.Parallel()
	k := tsh{tool: config.Tool{Bin: "/opt/tsh", Home: "/h"}}
	argv, env := k.Command(config.Conn{Port: 1, User: "u", Tunnel: &config.Tunnel{Target: "db"}})
	assert.Equal(t, []string{"/opt/tsh", "proxy", "db", "--tunnel", "--port", "1", "--db-user=u", "--", "db"}, argv)
	assert.Contains(t, env, "TELEPORT_HOME=/h")
}

func TestParseTshDatabases(t *testing.T) {
	t.Parallel()
	type want struct {
		targets []Target
		err     string
	}
	tcs := []struct {
		name string
		out  string
		want want
	}{
		{
			"label name first and protocol mapped",
			`[{"metadata":{"name":"db-1","labels":{"teleport.internal/discovered-name":"orders"}},"spec":{"protocol":"mysql"},"users":{"allowed":["viewer"]}},` +
				`{"metadata":{"name":"pg-1"},"spec":{"protocol":"postgres"}}]`,
			want{
				targets: []Target{
					{Target: "db-1", Name: "orders", Client: "mysql", Users: []string{"viewer"}},
					{Target: "pg-1", Name: "pg-1"},
				},
			},
		},
		{"broken output", `{`, want{err: "couldn't parse tsh db ls output"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTshDatabases([]byte(tc.out))
			assert.Equal(t, tc.want.targets, got)
			assert.Equal(t, tc.want.err == "", err == nil, "error: %v", err)
			assert.Contains(t, errText(err), tc.want.err)
		})
	}
}
