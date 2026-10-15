package clients

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

func TestSplitWords(t *testing.T) {
	t.Parallel()
	type want struct {
		words []string
		err   bool
	}
	tcs := []struct {
		in   string
		want want
	}{
		{`GET k`, want{words: []string{"GET", "k"}}},
		{`SET "a b" 'c d'`, want{words: []string{"SET", "a b", "c d"}}},
		{`HGET k "say \"hi\""`, want{words: []string{"HGET", "k", `say "hi"`}}},
		{`GET ""`, want{words: []string{"GET", ""}}},
		{`GET "open`, want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := splitWords(tc.in)
			if tc.want.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.words, got)
		})
	}
}

func TestRedisOptions(t *testing.T) {
	t.Parallel()
	type want struct {
		addr       string
		maxRetries int
		serverName string
	}
	tcs := []struct {
		name string
		conn config.Conn
		want want
	}{
		{"plain connection never retries a sent command", config.Conn{Host: "127.0.0.1", Port: 6379}, want{"127.0.0.1:6379", -1, ""}},
		{"tls uses the host as server name", config.Conn{Host: "cache.example", Port: 6380, TLS: true}, want{"cache.example:6380", -1, "cache.example"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt := redisOptions(tc.conn, "pw")
			serverName := ""
			if opt.TLSConfig != nil {
				serverName = opt.TLSConfig.ServerName
			}
			assert.Equal(t, tc.want, want{opt.Addr, opt.MaxRetries, serverName})
		})
	}
}

func TestRedisReply(t *testing.T) {
	t.Parallel()
	type want struct {
		records []string
		err     string
	}
	tcs := []struct {
		name  string
		reply any
		want  want
	}{
		{"map is one record", map[any]any{"b": "2", "a": int64(1)}, want{records: []string{`{"a":1,"b":"2"}`}}},
		{"nested map is converted", map[string]any{"m": map[any]any{"k": "v"}}, want{records: []string{`{"m":{"k":"v"}}`}}},
		{"array gives index and value", []any{"x", nil}, want{records: []string{`{"index":0,"value":"x"}`, `{"index":1,"value":null}`}}},
		{"scalar is one value", "OK", want{records: []string{`{"value":"OK"}`}}},
		{"an error inside an array", []any{"x", errors.New("WRONGTYPE")}, want{err: "redis 오류 응답"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, err := redisValue(tc.reply)
			if tc.want.err != "" {
				assert.ErrorContains(t, err, tc.want.err)
				return
			}
			require.NoError(t, err)
			recs := redisRecords(v)
			require.Len(t, recs, len(tc.want.records))
			for i, r := range recs {
				assert.Equal(t, tc.want.records[i], string(r.JSON()))
			}
		})
	}
}
