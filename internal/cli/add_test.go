package cli_test

import (
	"fmt"
	"sync"
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
		{
			"tunnel gets the stable port of its target and user",
			[]string{"add", "new", "--client", "mysql", "--tunnel", "tsh", "--target", "db-a", "--user", "viewer"},
			want{
				conn: config.Conn{
					Client: "mysql",
					Host:   config.DefaultHost,
					Port:   22913,
					User:   "viewer",
					Tunnel: &config.Tunnel{Type: "tsh", Target: "db-a"},
				},
			},
		},
		{"duplicate alias", []string{"add", "db", "--client", "mysql", "--port", "1"},
			want{code: 1, err: "alias already exists"}},
		{"port used on the same host", []string{"add", "r", "--client", "redis", "--port", "22912"},
			want{code: 1, err: "port used by another connection"}},
		{"same port on another host", []string{"add", "r", "--client", "redis", "--host", "192.0.2.1", "--port", "22912"},
			want{conn: config.Conn{Client: "redis", Host: "192.0.2.1", Port: 22912}}},
		{"tunnel needs target and user", []string{"add", "t", "--client", "mysql", "--tunnel", "tsh"},
			want{code: 1, err: "needs --target and --user"}},
		{"networked client needs a port", []string{"add", "p", "--client", "redis"}, want{code: 1, err: "needs --port"}},
		{"dynamodb refuses a tunnel", []string{"add", "d", "--client", "dynamodb", "--tunnel", "tsh", "--target", "x"},
			want{code: 1, err: "connects without a tunnel"}},
		{"dynamodb has no host or port",
			[]string{"add", "d", "--client", "dynamodb", "--profile", "p", "--region", "us-east-1"},
			want{conn: config.Conn{Client: "dynamodb", Profile: "p", Region: "us-east-1"}}},
		{"missing client", []string{"add", "x"},
			want{code: 1, err: "--client required\navailable: athena dynamodb kafka mysql redis"}},
		{"unknown client", []string{"add", "x", "--client", "mongo"},
			want{code: 1, err: "unsupported client: mongo\navailable: "}},
		{"dynamodb refuses --port", []string{"add", "d", "--client", "dynamodb", "--port", "8000"},
			want{code: 1, err: "dynamodb connects without --host or --port"}},
		{"athena refuses --host", []string{"add", "a", "--client", "athena", "--workgroup", "primary", "--host", "192.0.2.1"},
			want{code: 1, err: "athena connects without --host or --port"}},
		{"unknown tunnel", []string{"add", "x", "--client", "mysql", "--tunnel", "ssh"},
			want{code: 1, err: "unsupported tunnel"}},
		{"port out of range", []string{"add", "r", "--client", "redis", "--port", "70000"},
			want{code: 1, err: "--port must be between 1 and 65535"}},
		{"an explicit port 0 is refused", []string{"add", "r", "--client", "redis", "--port", "0"},
			want{code: 1, err: "--port must be between 1 and 65535"}},
		{"alias all is reserved", []string{"add", "all", "--client", "redis", "--port", "6379"},
			want{code: 1, err: "alias all is reserved for close all"}},
		{"target needs a tunnel", []string{"add", "r", "--client", "redis", "--port", "6379", "--target", "x"},
			want{code: 1, err: "--target needs --tunnel"}},
		{"mysql needs a user", []string{"add", "m", "--client", "mysql", "--port", "3306"},
			want{code: 1, err: "mysql connection is missing: user"}},
		{"kafka secret needs a user",
			[]string{"add", "k", "--client", "kafka", "--port", "9092", "--secret", "env:GATEREACH_KAFKA_PW"},
			want{code: 1, err: "a kafka secret needs a user"}},
		{"dynamodb refuses --tls", []string{"add", "d", "--client", "dynamodb", "--tls"},
			want{code: 1, err: "connects without --tls"}},
		{
			"athena has no host or port",
			[]string{"add", "a", "--client", "athena", "--region", "us-east-1", "--workgroup", "primary",
				"--output-location", "s3://example-bucket/results/", "--catalog", "acme_catalog", "--database", "acme"},
			want{conn: config.Conn{Client: "athena", Region: "us-east-1", Workgroup: "primary",
				OutputLocation: "s3://example-bucket/results/", Catalog: "acme_catalog", Database: "acme"}},
		},
		{"athena needs a workgroup", []string{"add", "a", "--client", "athena"},
			want{code: 1, err: "athena connection is missing: workgroup"}},
		{
			"athena output location must be s3",
			[]string{"add", "a", "--client", "athena", "--workgroup", "primary", "--output-location", "example-bucket"},
			want{code: 1, err: "athena output_location must start with s3://"},
		},
		{"only athena takes a workgroup", []string{"add", "a", "--client", "dynamodb", "--workgroup", "primary"},
			want{code: 1, err: "dynamodb doesn't use --workgroup, --output-location or --catalog"}},
		{"athena refuses a tunnel", []string{"add", "a", "--client", "athena", "--tunnel", "tsh", "--target", "x"},
			want{code: 1, err: "connects without a tunnel"}},
		{"an env secret outside the prefix", []string{"add", "r", "--client", "redis", "--port", "6379",
			"--secret", "env:AWS_SECRET_ACCESS_KEY"}, want{code: 1, err: "env secret must name a GATEREACH_ variable"}},
		{"a password in place of a reference", []string{"add", "r", "--client", "redis", "--port", "6379",
			"--secret", "hunter2"},
			want{code: 1, err: "unsupported secret reference\nuse keychain:<item> or env:GATEREACH_<name>\n"}},
		{"a secret another alias uses", []string{"add", "m", "--client", "mysql", "--port", "3307", "--user", "viewer",
			"--secret", "keychain:acme-db"},
			want{code: 1, err: "secret keychain:acme-db already belongs to alias db\nreuse that alias instead"}},
		{"a secret to a remote host needs tls", []string{"add", "r", "--client", "redis", "--host", "192.0.2.1",
			"--port", "6379", "--secret", "keychain:acme-cache"},
			want{code: 1, err: "redis --secret to 192.0.2.1 needs --tls\nadd --tls or reach it through --tunnel"}},
		{"a secret to a remote host over tls", []string{"add", "r", "--client", "redis", "--host", "192.0.2.1",
			"--port", "6379", "--secret", "keychain:acme-cache", "--tls"},
			want{conn: config.Conn{Client: "redis", Host: "192.0.2.1", Port: 6379, Secret: "keychain:acme-cache", TLS: true}}},
		{"a secret to localhost needs no tls", []string{"add", "r", "--client", "redis", "--host", "localhost",
			"--port", "6379", "--secret", "keychain:acme-cache"},
			want{conn: config.Conn{Client: "redis", Host: "localhost", Port: 6379, Secret: "keychain:acme-cache"}}},
		{"a secret through a tunnel needs no tls", []string{"add", "m", "--client", "mysql", "--tunnel", "tsh",
			"--target", "db-b", "--user", "viewer", "--secret", "keychain:acme-db-b"},
			want{conn: config.Conn{Client: "mysql", Host: config.DefaultHost, Port: 26883, User: "viewer",
				Secret: "keychain:acme-db-b", Tunnel: &config.Tunnel{Type: "tsh", Target: "db-b"}}}},
		{"a user that reads as a flag", []string{"add", "m", "--client", "mysql", "--tunnel", "tsh", "--target", "db-a",
			"--user=--proxy=example.com"}, want{code: 1, err: "--user can't start with - or hold spaces or control characters"}},
		{"a target with a space", []string{"add", "m", "--client", "mysql", "--tunnel", "tsh", "--target", "db a",
			"--user", "viewer"}, want{code: 1, err: "--target can't start with - or hold spaces or control characters"}},
		{"a host with a control character", []string{"add", "r", "--client", "redis", "--host", "192.0.2.1\x1b",
			"--port", "6379"}, want{code: 1, err: "--host can't start with - or hold spaces or control characters"}},
		{"add can't turn --write on", []string{"add", "r", "--client", "redis", "--port", "6379", "--write"},
			want{code: 2, err: "flag provided but not defined: -write"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t, existing+"secret = \"keychain:acme-db\"\n")
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

func TestAdd_SecretValueNotRepeated(t *testing.T) {
	useConfig(t, "")
	code, out, errw := run("add", "r", "--client", "redis", "--port", "6379", "--secret", "hunter2")
	assert.Equal(t, 1, code)
	assert.NotContains(t, out+errw, "hunter2")
}

// Each add holds the lock from its checks to its append
// so adds at once never take the same alias or port
func TestAdd_Concurrent(t *testing.T) {
	useConfig(t, "")
	const n = 8
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			codes[i], _, _ = run("add", fmt.Sprintf("t%d", i), "--client", "mysql",
				"--tunnel", "tsh", "--target", "db-a", "--user", "viewer")
		})
	}
	wg.Wait()
	assert.Equal(t, make([]int, n), codes)
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Len(t, cfg.Aliases, n)
	assert.Len(t, cfg.UsedPorts(config.DefaultHost), n)
}
