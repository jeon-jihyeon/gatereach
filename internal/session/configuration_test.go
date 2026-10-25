package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

func TestQueryConfiguration(t *testing.T) {
	base := config.Conn{Client: "mock"}
	writable := config.Conn{Client: "mock", Write: true}
	type args struct {
		conn  config.Conn
		hash  string
		write bool
	}
	type want struct {
		rows    int
		changed bool
		err     bool
	}
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"same configuration", args{conn: base, hash: base.Fingerprint()}, want{rows: 1}},
		{"missing fingerprint", args{conn: base}, want{changed: true, err: true}},
		{"changed target", args{conn: base, hash: (config.Conn{Client: "mock", Host: "192.0.2.1"}).Fingerprint()},
			want{changed: true, err: true}},
		{"changed write permission", args{conn: writable, hash: base.Fingerprint()}, want{changed: true, err: true}},
		{"direct write refused", args{conn: base, hash: base.Fingerprint(), write: true}, want{err: true}},
		{"saved write permission", args{conn: writable, hash: writable.Fingerprint(), write: true}, want{rows: 1}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			held := connected(t)
			reads(held).DoAndReturn(emitRows).Times(tc.want.rows)
			serveConn(t, tc.args.conn, waits{busy: time.Second, request: requestWait}, nil, held)
			end, err := Query(context.Background(), Request{
				Alias: testAlias, Version: testVersion, Config: tc.args.hash,
				Query: "x", Limit: 1, Write: tc.args.write,
			}, func([]byte) error { return nil })
			assert.Equal(t, tc.want.err, err != nil)
			assert.Equal(t, tc.want.rows, end.Rows)
			assert.Equal(t, tc.want.changed, end.ConfigChanged)
			assert.Equal(t, tc.want.changed, errors.Is(err, ErrConfigChanged))
		})
	}
}
