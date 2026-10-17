package clients

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
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
		{`GET 'a\b'`, want{words: []string{"GET", `a\b`}}},
		{"GET  k\t\n", want{words: []string{"GET", "k"}}},
		{`GET "open`, want{err: true}},
		{`GET 'open`, want{err: true}},
		{`GET k\`, want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := splitWords(tc.in)
			assert.Equal(t, tc.want, want{words: got, err: err != nil})
		})
	}
}

func TestRedisOptions(t *testing.T) {
	t.Parallel()
	type want struct {
		addr           string
		maxRetries     int
		serverName     string
		contextTimeout bool
	}
	tcs := []struct {
		name string
		conn config.Conn
		want want
	}{
		{"plain connection never retries a sent command and keeps the deadline", config.Conn{Host: "127.0.0.1", Port: 6379},
			want{"127.0.0.1:6379", -1, "", true}},
		{"tls uses the host as server name", config.Conn{Host: "cache.example", Port: 6380, TLS: true},
			want{"cache.example:6380", -1, "cache.example", true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt := redisOptions(tc.conn, "pw")
			serverName := ""
			if opt.TLSConfig != nil {
				serverName = opt.TLSConfig.ServerName
			}
			assert.Equal(t, tc.want, want{opt.Addr, opt.MaxRetries, serverName, opt.ContextTimeoutEnabled})
		})
	}
}

// The whole reply counts before limit cuts it
// and the records under limit come out before the budget
func TestRedisReplyUse(t *testing.T) {
	t.Parallel()
	type want struct {
		used    []float64
		emitted int
		err     error
	}
	tcs := []struct {
		name  string
		reply any
		limit int
		// Replies of the commands that bounded the read
		counted float64
		useErr  error
		want    want
	}{
		{"a scalar counts 1", "OK", 10, 0, nil, want{used: []float64{1}, emitted: 1}},
		{"a nil reply counts 1", nil, 10, 0, nil, want{used: []float64{1}, emitted: 1}},
		{"each array element counts", []any{"a", "b", "c"}, 10, 0, nil, want{used: []float64{3}, emitted: 3}},
		{"nested arrays count what they hold", []any{"id", []any{"f", "v", "g", "w"}}, 10, 0, nil,
			want{used: []float64{5}, emitted: 2}},
		{"each map value counts", map[any]any{"a": "1", "b": []any{"x", "y"}}, 10, 0, nil,
			want{used: []float64{3}, emitted: 1}},
		{"an empty array counts nothing", []any{}, 10, 0, nil, want{used: []float64{0}}},
		{"the count ignores the limit", []any{"a", "b", "c"}, 1, 0, nil, want{used: []float64{3}, emitted: 1}},
		{"the paid records come out before the budget", []any{"a", "b", "c"}, 2, 0, ErrBudget,
			want{used: []float64{3}, emitted: 2, err: ErrBudget}},
		{"counting replies go into the same amount", []any{"a", "b"}, 10, 1, nil, want{used: []float64{3}, emitted: 2}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var used []float64
			emitted := 0
			q := Query{Limit: tc.limit, Use: func(amount float64) error {
				used = append(used, amount)
				return tc.useErr
			}}
			err := redisReply(tc.reply, q, tc.counted, func(record.Record) error {
				emitted++
				return nil
			})
			assert.Equal(t, tc.want, want{used: used, emitted: emitted, err: err})
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
		{"array gives index and value", []any{"x", nil},
			want{records: []string{`{"index":0,"value":"x"}`, `{"index":1,"value":null}`}}},
		{"scalar is one value", "OK", want{records: []string{`{"value":"OK"}`}}},
		{"an error inside an array", []any{"x", errors.New("WRONGTYPE")}, want{err: "redis error reply: WRONGTYPE"}},
		{"an error inside a map", map[any]any{"k": errors.New("WRONGTYPE")}, want{err: "redis error reply: WRONGTYPE"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var records []string
			err := redisReply(tc.reply, Query{Limit: 10}, 0, func(r record.Record) error {
				records = append(records, string(r.JSON()))
				return nil
			})
			assert.Equal(t, tc.want, want{records: records, err: errText(err)})
		})
	}
}

// The server's work stays near limit before a read-only command runs
func TestRedisBound(t *testing.T) {
	t.Parallel()
	// Sizes the fake server reports per key
	sizes := map[string]int64{"big": 50, "small": 3, "ten": 10}
	type want struct {
		words []string
		// Cardinality commands sent
		asked   []string
		counted int
		err     string
	}
	tcs := []struct {
		name string
		cmd  string
		want want
	}{
		{"a small hash passes", "HGETALL small",
			want{words: []string{"HGETALL", "small"}, asked: []string{"HLEN small"}, counted: 1}},
		{"a hash of exactly limit passes", "hkeys ten",
			want{words: []string{"hkeys", "ten"}, asked: []string{"HLEN ten"}, counted: 1}},
		{"a big hash is refused", "HGETALL big", want{asked: []string{"HLEN big"}, counted: 1,
			err: "big holds 50 items, more than --limit 10\nuse HSCAN to page through it"}},
		{"a big set is refused", "SMEMBERS big", want{asked: []string{"SCARD big"}, counted: 1,
			err: "big holds 50 items, more than --limit 10\nuse SSCAN to page through it"}},
		{"HVALS counts with HLEN", "HVALS big", want{asked: []string{"HLEN big"}, counted: 1,
			err: "big holds 50 items, more than --limit 10\nuse HSCAN to page through it"}},
		{"wrong arguments go to the server", "HGETALL", want{words: []string{"HGETALL"}}},
		{"a short index range needs no size", "LRANGE big 0 9", want{words: []string{"LRANGE", "big", "0", "9"}}},
		{"a reversed index range is empty", "LRANGE big 9 0", want{words: []string{"LRANGE", "big", "9", "0"}}},
		{"a long index range on a short list passes", "LRANGE small 0 100",
			want{words: []string{"LRANGE", "small", "0", "100"}, asked: []string{"LLEN small"}, counted: 1}},
		{"a long index range is refused", "LRANGE big 0 100", want{asked: []string{"LLEN big"}, counted: 1,
			err: "LRANGE asks for 50 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"a negative stop counts from the end", "LRANGE big 0 -1", want{asked: []string{"LLEN big"}, counted: 1,
			err: "LRANGE asks for 50 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"a negative range within limit passes", "LRANGE big -10 -1",
			want{words: []string{"LRANGE", "big", "-10", "-1"}, asked: []string{"LLEN big"}, counted: 1}},
		{"the largest stop does not overflow", "LRANGE big 0 9223372036854775807", want{asked: []string{"LLEN big"},
			counted: 1, err: "LRANGE asks for 50 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"ZREVRANGE counts with ZCARD", "ZREVRANGE big 0 -1 WITHSCORES", want{asked: []string{"ZCARD big"}, counted: 1,
			err: "ZREVRANGE asks for 50 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"ZRANGE by index counts with ZCARD", "ZRANGE big 0 -1 REV", want{asked: []string{"ZCARD big"}, counted: 1,
			err: "ZRANGE asks for 50 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"ZRANGE BYSCORE gets a LIMIT", "ZRANGE big 0 100 BYSCORE",
			want{words: []string{"ZRANGE", "big", "0", "100", "BYSCORE", "LIMIT", "0", "10"}}},
		{"ZRANGEBYSCORE gets a LIMIT", "ZRANGEBYSCORE big -inf +inf WITHSCORES",
			want{words: []string{"ZRANGEBYSCORE", "big", "-inf", "+inf", "WITHSCORES", "LIMIT", "0", "10"}}},
		{"ZRANGEBYSCORE keeps a LIMIT within limit", "ZRANGEBYSCORE big 0 1 LIMIT 5 10",
			want{words: []string{"ZRANGEBYSCORE", "big", "0", "1", "LIMIT", "5", "10"}}},
		{"ZRANGEBYSCORE refuses a LIMIT past limit", "ZRANGEBYSCORE big 0 1 limit 0 11", want{
			err: "ZRANGEBYSCORE LIMIT count 11 reads more than --limit 10\nuse a count from 0 to 10"}},
		{"ZRANGE BYLEX refuses a negative LIMIT count", "ZRANGE big [a [z BYLEX LIMIT 0 -1", want{
			err: "ZRANGE LIMIT count -1 reads more than --limit 10\nuse a count from 0 to 10"}},
		{"ZRANGE with an unknown option goes to the server", "ZRANGE big 0 -1 BYSCORE NOPE",
			want{words: []string{"ZRANGE", "big", "0", "-1", "BYSCORE", "NOPE"}}},
		{"XRANGE gets a COUNT", "XRANGE s - +", want{words: []string{"XRANGE", "s", "-", "+", "COUNT", "10"}}},
		{"XREVRANGE keeps a COUNT within limit", "XREVRANGE s + - COUNT 10",
			want{words: []string{"XREVRANGE", "s", "+", "-", "COUNT", "10"}}},
		{"XRANGE refuses a COUNT past limit", "XRANGE s - + count 100", want{
			err: "XRANGE asks for 100 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"SRANDMEMBER within limit passes", "SRANDMEMBER big -10",
			want{words: []string{"SRANDMEMBER", "big", "-10"}}},
		{"SRANDMEMBER past limit is refused", "SRANDMEMBER big -11", want{
			err: "SRANDMEMBER asks for 11 items, more than --limit 10\nnarrow the range or raise --limit"}},
		{"a command with no bound goes as is", "GET k", want{words: []string{"GET", "k"}}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			words, err := splitWords(tc.cmd)
			require.NoError(t, err)
			var asked []string
			got, counted, err := redisBound(words, 10, func(command, key string) (int64, error) {
				asked = append(asked, command+" "+key)
				return sizes[key], nil
			})
			assert.Equal(t, tc.want, want{words: got, asked: asked, counted: counted, err: errText(err)})
		})
	}
}

// A failed size reply ends the read with its error and counts nothing
func TestRedisBoundSizeError(t *testing.T) {
	t.Parallel()
	_, counted, err := redisBound([]string{"HGETALL", "k"}, 10, func(string, string) (int64, error) {
		return 0, errors.New("redis error reply: WRONGTYPE")
	})
	assert.Equal(t, 0, counted)
	assert.EqualError(t, err, "redis error reply: WRONGTYPE")
}

func TestRedisScanCount(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		cmd  string
		want string
	}{
		{"SCAN 0", ""},
		{"SCAN 0 COUNT 1000 TYPE hash", ""},
		{"SCAN 0 COUNT 1001", "read-only mode allows SCAN COUNT up to 1000\npage through it with the cursor instead"},
		{"zscan z 0 match * count 2000",
			"read-only mode allows ZSCAN COUNT up to 1000\npage through it with the cursor instead"},
		{"HSCAN h 0 MATCH COUNT COUNT 5", ""},
		{"SSCAN s 0 COUNT 10 COUNT 99999",
			"read-only mode allows SSCAN COUNT up to 1000\npage through it with the cursor instead"},
	}
	for _, tc := range tcs {
		t.Run(tc.cmd, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, errText(redisCheck(tc.cmd, false)))
		})
	}
}

// An array reply builds no record past limit
func TestRedisRecordsLimit(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name  string
		reply any
		limit int
		want  int
	}{
		{"an array is cut at limit", []any{"a", "b", "c"}, 2, 2},
		{"a short array keeps every element", []any{"a"}, 2, 1},
		{"a map stays one record", map[string]any{"a": "1", "b": "2"}, 1, 1},
		{"a scalar stays one record", "OK", 1, 1},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Len(t, redisRecords(tc.reply, tc.limit), tc.want)
		})
	}
}
