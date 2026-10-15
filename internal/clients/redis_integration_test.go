//go:build integration

package clients

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 로컬 redis-server가 있을 때만 실행
func TestRedisAgainstALocalServer(t *testing.T) {
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	srv := exec.Command(bin, "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Process.Kill(); _ = srv.Wait() })

	c := newRedis(config.Conn{Client: "redis", Host: "127.0.0.1", Port: port}, "")
	ctx := context.Background()
	require.Eventually(t, func() bool { return c.Connect(ctx) == nil }, 5*time.Second, 50*time.Millisecond)
	defer c.Close()

	var got []record.Record
	emit := func(r record.Record) error { got = append(got, r); return nil }
	require.NoError(t, c.Read(ctx, Query{Text: "HSET h a 1 b x", Write: true, Limit: 10}, emit))
	got = nil
	require.NoError(t, c.Read(ctx, Query{Text: "HGETALL h", Limit: 10}, emit))
	require.Len(t, got, 1)
	assert.Equal(t, `{"a":"1","b":"x"}`, string(got[0].JSON()))
	got = nil
	require.NoError(t, c.Read(ctx, Query{Text: "GET missing", Limit: 10}, emit))
	assert.Equal(t, `{"value":null}`, string(got[0].JSON()))
	err = c.Read(ctx, Query{Text: "HGETALL", Limit: 10}, emit)
	assert.ErrorContains(t, err, "redis 오류 응답")
}
