package config_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

func TestConnFingerprint(t *testing.T) {
	t.Parallel()
	type args struct{ first, second config.Conn }
	tcs := []struct {
		name string
		args args
		same bool
	}{
		{"empty connection", args{}, true},
		{"client", args{second: config.Conn{Client: "redis"}}, false},
		{"host", args{second: config.Conn{Host: "192.0.2.1"}}, false},
		{"port", args{second: config.Conn{Port: 6379}}, false},
		{"user", args{second: config.Conn{User: "ann"}}, false},
		{"database", args{second: config.Conn{Database: "acme"}}, false},
		{"profile", args{second: config.Conn{Profile: "acme"}}, false},
		{"region", args{second: config.Conn{Region: "acme"}}, false},
		{"secret reference", args{second: config.Conn{Secret: "env:GATEREACH_ACME"}}, false},
		{"tls", args{second: config.Conn{TLS: true}}, false},
		{"write permission", args{second: config.Conn{Write: true}}, false},
		{"workgroup", args{second: config.Conn{Workgroup: "acme"}}, false},
		{"output location", args{second: config.Conn{OutputLocation: "s3://acme/"}}, false},
		{"catalog", args{second: config.Conn{Catalog: "acme"}}, false},
		{"tunnel", args{second: config.Conn{Tunnel: &config.Tunnel{Type: "tsh", Target: "acme"}}}, false},
		{"equal tunnel values", args{
			first:  config.Conn{Tunnel: &config.Tunnel{Type: "tsh", Target: "acme"}},
			second: config.Conn{Tunnel: &config.Tunnel{Type: "tsh", Target: "acme"}},
		}, true},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := tc.args.first.Fingerprint()
			second := tc.args.second.Fingerprint()
			assert.Equal(t, tc.same, first == second)
			decoded, err := hex.DecodeString(first)
			require.NoError(t, err)
			assert.Len(t, decoded, 32)
		})
	}
}
