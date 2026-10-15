package cli_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

func TestAdd(t *testing.T) {
	type want struct {
		code int
		err  string
		conn config.Conn
	}
	tcs := []struct {
		name string
		args []string
		want want
	}{
		{"tunnel gets the stable port of its target and user", []string{"add", "new", "--client", "mysql", "--tunnel", "tsh", "--target", "db-a", "--user", "viewer"},
			want{conn: config.Conn{Client: "mysql", Host: config.DefaultHost, Port: 22913, User: "viewer", Tunnel: &config.Tunnel{Type: "tsh", Target: "db-a"}}}},
		{"duplicate alias", []string{"add", "db", "--client", "mysql", "--port", "1"}, want{code: 1, err: "이미 있는 별칭"}},
		{"port used on the same host", []string{"add", "r", "--client", "redis", "--port", "22912"}, want{code: 1, err: "다른 연결이 쓰는 포트"}},
		{"same port on another host", []string{"add", "r", "--client", "redis", "--host", "10.0.0.1", "--port", "22912"},
			want{conn: config.Conn{Client: "redis", Host: "10.0.0.1", Port: 22912}}},
		{"tunnel needs target and user", []string{"add", "t", "--client", "mysql", "--tunnel", "tsh"}, want{code: 1, err: "--target과 --user 필요"}},
		{"networked client needs a port", []string{"add", "p", "--client", "redis"}, want{code: 1, err: "--port 필요"}},
		{"dynamodb refuses a tunnel", []string{"add", "d", "--client", "dynamodb", "--tunnel", "tsh", "--target", "x"}, want{code: 1, err: "터널 없이 연결"}},
		{"dynamodb has no host or port", []string{"add", "d", "--client", "dynamodb", "--profile", "p", "--region", "ap-northeast-2"},
			want{conn: config.Conn{Client: "dynamodb", Profile: "p", Region: "ap-northeast-2"}}},
		{"unknown client", []string{"add", "x", "--client", "mongo"}, want{code: 1, err: "--client 필요"}},
		{"unknown tunnel", []string{"add", "x", "--client", "mysql", "--tunnel", "ssh"}, want{code: 1, err: "지원하지 않는 터널"}},
		{"port out of range", []string{"add", "r", "--client", "redis", "--port", "70000"}, want{code: 1, err: "--port는 1부터 65535까지"}},
		{"mysql needs a user", []string{"add", "m", "--client", "mysql", "--port", "3306"}, want{code: 1, err: "mysql 연결 설정에 없는 키: user"}},
		{"kafka secret needs a user", []string{"add", "k", "--client", "kafka", "--port", "9092", "--secret", "env:KAFKA_PW"},
			want{code: 1, err: "kafka secret을 쓰려면 user 필요"}},
		{"dynamodb refuses --tls", []string{"add", "d", "--client", "dynamodb", "--tls"}, want{code: 1, err: "--tls 없이 연결"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, existing)
			code, _, errw := run(tc.args...)
			assert.Equal(t, tc.want.code, code, errw)
			if tc.want.err != "" {
				assert.Contains(t, errw, tc.want.err)
				return
			}
			cfg, err := config.Load()
			require.NoError(t, err)
			assert.Equal(t, tc.want.conn, cfg.Conns[tc.args[1]])
		})
	}
}
