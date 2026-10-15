package tunnels

import (
	"errors"
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
		err  bool
	}
	tcs := []struct {
		name string
		out  string
		err  error
		want want
	}{
		{"matches only the exact command line", " 11 /opt/tsh proxy db --tunnel --port 21000 --db-user viewer db\n" +
			" 12 /opt/tsh proxy db --tunnel --port 21000 --db-user viewer db --debug\n" +
			" 13 Teleport Connect\n", nil, want{pids: []int{11}}},
		{"ps fails", "", errors.New("no ps"), want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k := tsh{tool: config.Tool{Bin: "/opt/tsh"}, ps: func() ([]byte, error) { return []byte(tc.out), tc.err }}
			got, err := k.Pids(c)
			if tc.want.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.pids, got)
		})
	}
}

func TestTshCommand(t *testing.T) {
	t.Parallel()
	k := tsh{tool: config.Tool{Bin: "/opt/tsh", Home: "/h"}}
	argv, env := k.Command(config.Conn{Port: 1, User: "u", Tunnel: &config.Tunnel{Target: "db"}})
	assert.Equal(t, []string{"/opt/tsh", "proxy", "db", "--tunnel", "--port", "1", "--db-user", "u", "db"}, argv)
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
		{"label name first and protocol mapped", `[{"metadata":{"name":"db-1","labels":{"teleport.internal/discovered-name":"orders"}},"spec":{"protocol":"mysql"},"users":{"allowed":["viewer"]}},` +
			`{"metadata":{"name":"pg-1"},"spec":{"protocol":"postgres"}}]`,
			want{targets: []Target{{Target: "db-1", Name: "orders", Client: "mysql", Users: []string{"viewer"}}, {Target: "pg-1", Name: "pg-1"}}}},
		{"broken output", `{`, want{err: "출력 해석 실패"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTshDatabases([]byte(tc.out))
			if tc.want.err != "" {
				assert.ErrorContains(t, err, tc.want.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.targets, got)
		})
	}
}
