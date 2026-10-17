package clients

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Commands read-only mode allows
// KEYS can block the server so it's left out in favor of SCAN
var redisRead = map[string]bool{
	"GET": true, "MGET": true, "GETRANGE": true, "STRLEN": true, "EXISTS": true, "TYPE": true, "TTL": true, "PTTL": true,
	"HGET": true, "HMGET": true, "HGETALL": true, "HKEYS": true, "HVALS": true, "HLEN": true, "HEXISTS": true, "HSCAN": true,
	"LRANGE": true, "LLEN": true, "LINDEX": true,
	"SMEMBERS": true, "SISMEMBER": true, "SCARD": true, "SRANDMEMBER": true, "SSCAN": true,
	"ZRANGE": true, "ZRANGEBYSCORE": true, "ZREVRANGE": true, "ZSCORE": true, "ZCARD": true, "ZCOUNT": true, "ZRANK": true, "ZSCAN": true,
	"XRANGE": true, "XREVRANGE": true, "XLEN": true,
	"SCAN": true, "DBSIZE": true, "INFO": true, "PING": true, "TIME": true,
}

type redisClient struct {
	conn     config.Conn
	password string
	rdb      *redis.Client
}

func newRedis(conn config.Conn, password string) Client {
	return &redisClient{conn: conn, password: password}
}

func redisCheck(text string, write bool) error {
	words, err := splitWords(text)
	if err != nil {
		return err
	}
	if len(words) == 0 {
		return errors.New("empty command")
	}
	if write {
		return nil
	}
	if !redisRead[strings.ToUpper(words[0])] {
		return fmt.Errorf("command blocked in read-only mode: %s\nuse --write to write", words[0])
	}
	return redisScanCount(words)
}

// The most a SCAN family command may ask for in one call
const redisScanMax = 1000

// COUNT sets how many slots one SCAN call walks
// so a large one blocks the server like KEYS
func redisScanCount(words []string) error {
	cmd := strings.ToUpper(words[0])
	from := 3
	switch cmd {
	case "SCAN":
		from = 2
	case "HSCAN", "SSCAN", "ZSCAN":
	default:
		return nil
	}
	for i := from; i < len(words); i++ {
		switch strings.ToUpper(words[i]) {
		case "COUNT":
			if i+1 < len(words) {
				if n, err := strconv.ParseInt(words[i+1], 10, 64); err == nil && n > redisScanMax {
					return fmt.Errorf("read-only mode allows %s COUNT up to %d\npage through it with the cursor instead",
						cmd, redisScanMax)
				}
			}
			i++
		case "MATCH", "TYPE":
			i++
		}
	}
	return nil
}

// Reads that return a whole collection with the command that counts it and the one that pages through it
var redisWhole = map[string]struct{ size, scan string }{
	"HGETALL": {"HLEN", "HSCAN"}, "HKEYS": {"HLEN", "HSCAN"}, "HVALS": {"HLEN", "HSCAN"}, "SMEMBERS": {"SCARD", "SSCAN"},
}

// Counts the items of one key with a command such as HLEN
type redisSize func(command, key string) (int64, error)

// Bounds a read-only command by limit before it runs so the server never walks a large key for a cut reply
// It returns the command to send and how many counting replies it took
// 1. A whole collection read is refused when the key holds more than limit items
// 2. An index range is refused when it covers more than limit items
// 3. A score or lex range needs a LIMIT count from 0 to limit and gets `LIMIT 0 <limit>` without one
// 4. A stream range needs a COUNT up to limit and gets `COUNT <limit>` without one
// 5. SRANDMEMBER asks for at most limit members
// 6. A command with the wrong arguments goes as is since the server refuses it before any work
func redisBound(words []string, limit int, size redisSize) ([]string, int, error) {
	cmd := strings.ToUpper(words[0])
	if w, ok := redisWhole[cmd]; ok && len(words) == 2 {
		n, err := size(w.size, words[1])
		if err != nil {
			return nil, 0, err
		}
		if n > int64(limit) {
			return nil, 1, fmt.Errorf("%s holds %d items, more than --limit %d\nuse %s to page through it",
				words[1], n, limit, w.scan)
		}
		return words, 1, nil
	}
	switch cmd {
	case "SRANDMEMBER":
		if len(words) == 3 {
			if n, err := strconv.ParseInt(words[2], 10, 64); err == nil && (n > int64(limit) || n < -int64(limit)) {
				return nil, 0, redisTooMany(cmd, n, limit)
			}
		}
	case "LRANGE":
		if len(words) == 4 {
			return redisIndexRange(words, limit, "LLEN", size)
		}
	case "ZREVRANGE":
		if len(words) == 4 || len(words) == 5 && strings.EqualFold(words[4], "WITHSCORES") {
			return redisIndexRange(words, limit, "ZCARD", size)
		}
	case "ZRANGE", "ZRANGEBYSCORE":
		if len(words) >= 4 {
			return redisZRange(words, limit, size)
		}
	case "XRANGE", "XREVRANGE":
		if len(words) >= 4 {
			return redisCounted(words, limit)
		}
	}
	return words, 0, nil
}

func redisTooMany(cmd string, n int64, limit int) error {
	if n < 0 {
		n = -n
	}
	return fmt.Errorf("%s asks for %d items, more than --limit %d\nnarrow the range or raise --limit", cmd, n, limit)
}

// Items of `<command> key start stop` once negative indexes count from the end
// The key's size is asked only when the indexes alone may cover more than limit
func redisIndexRange(words []string, limit int, sizeCmd string, size redisSize) ([]string, int, error) {
	start, err1 := strconv.ParseInt(words[2], 10, 64)
	stop, err2 := strconv.ParseInt(words[3], 10, 64)
	if err1 != nil || err2 != nil {
		return words, 0, nil
	}
	if start >= 0 && stop >= 0 && (stop < start || stop-start < int64(limit)) {
		return words, 0, nil
	}
	n, err := size(sizeCmd, words[1])
	if err != nil {
		return nil, 0, err
	}
	if start < 0 {
		start = max(start+n, 0)
	}
	if stop < 0 {
		stop += n
	}
	stop = min(stop, n-1)
	if stop >= start && stop-start >= int64(limit) {
		return nil, 1, redisTooMany(strings.ToUpper(words[0]), stop-start+1, limit)
	}
	return words, 1, nil
}

// ZRANGE by index counts like LRANGE while a score or lex range is bounded by its LIMIT
// The options are read the way the server reads them
func redisZRange(words []string, limit int, size redisSize) ([]string, int, error) {
	cmd := strings.ToUpper(words[0])
	byIndex := cmd == "ZRANGE"
	var counts []string
	for i := 4; i < len(words); i++ {
		switch opt := strings.ToUpper(words[i]); {
		case opt == "WITHSCORES":
		case opt == "LIMIT" && i+2 < len(words):
			counts = append(counts, words[i+2])
			i += 2
		case cmd == "ZRANGE" && (opt == "BYSCORE" || opt == "BYLEX"):
			byIndex = false
		case cmd == "ZRANGE" && opt == "REV":
		default:
			return words, 0, nil
		}
	}
	if byIndex {
		if len(counts) > 0 {
			return words, 0, nil
		}
		return redisIndexRange(words, limit, "ZCARD", size)
	}
	if len(counts) == 0 {
		return append(slices.Clone(words), "LIMIT", "0", strconv.Itoa(limit)), 0, nil
	}
	for _, c := range counts {
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			return words, 0, nil
		}
		// A negative count returns the whole range
		if n < 0 || n > int64(limit) {
			return nil, 0, fmt.Errorf("%s LIMIT count %d reads more than --limit %d\nuse a count from 0 to %d",
				cmd, n, limit, limit)
		}
	}
	return words, 0, nil
}

// A stream range without COUNT returns every entry in it
func redisCounted(words []string, limit int) ([]string, int, error) {
	cmd := strings.ToUpper(words[0])
	counted := false
	for i := 4; i < len(words); i += 2 {
		if !strings.EqualFold(words[i], "COUNT") || i+1 == len(words) {
			return words, 0, nil
		}
		n, err := strconv.ParseInt(words[i+1], 10, 64)
		if err != nil {
			return words, 0, nil
		}
		if n > int64(limit) {
			return nil, 0, redisTooMany(cmd, n, limit)
		}
		counted = true
	}
	if !counted {
		return append(slices.Clone(words), "COUNT", strconv.Itoa(limit)), 0, nil
	}
	return words, 0, nil
}

func (r *redisClient) Connect(ctx context.Context) error {
	rdb := redis.NewClient(redisOptions(r.conn, r.password))
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return err
	}
	r.rdb = rdb
	return nil
}

func redisOptions(conn config.Conn, password string) *redis.Options {
	opt := &redis.Options{
		Addr:     net.JoinHostPort(conn.Host, strconv.Itoa(conn.Port)),
		Username: conn.User,
		Password: password,
		// The session sends single reads one at a time so one connection is enough
		PoolSize: 1,
		// Proxies don't know CLIENT SETINFO
		DisableIdentity: true,
		// The driver would also resend after an EOF that follows sending
		// so retries are off and a `--write` command never applies twice
		// The pool's health check reconnects a connection that dropped while idle
		MaxRetries: -1,
		// The driver ignores the context's deadline otherwise so --timeout would never cut a slow reply
		ContextTimeoutEnabled: true,
	}
	if conn.TLS {
		opt.TLSConfig = &tls.Config{ServerName: conn.Host, MinVersion: tls.VersionTLS12}
	}
	return opt
}

func (r *redisClient) Close() error {
	if r.rdb == nil {
		return nil
	}
	return r.rdb.Close()
}

// Without --write redisBound keeps the server's work near limit
// With --write the command goes as is and limit only cuts the reply after it arrives
func (r *redisClient) Read(ctx context.Context, q Query, emit Emit) error {
	if r.rdb == nil {
		return ErrDisconnected
	}
	words, err := splitWords(q.Text)
	if err != nil {
		return err
	}
	counted := 0
	if !q.Write && len(words) > 0 {
		var bound []string
		bound, counted, err = redisBound(words, q.Limit, func(command, key string) (int64, error) {
			n, err := r.rdb.Do(ctx, command, key).Int64()
			return n, redisError(err)
		})
		if err != nil {
			// A refused read still paid for the counting replies so the budget hears of them
			_ = q.use(float64(counted))
			return err
		}
		words = bound
	}
	args := make([]any, len(words))
	for i, w := range words {
		args[i] = w
	}
	reply, err := r.rdb.Do(ctx, args...).Result()
	if errors.Is(err, redis.Nil) {
		reply, err = nil, nil
	}
	if err := redisError(err); err != nil {
		return err
	}
	return redisReply(reply, q, float64(counted), emit)
}

// Only the server's own error is an error reply
// A dropped connection or a deadline keeps the driver's error
func redisError(err error) error {
	var serverErr redis.Error
	if errors.As(err, &serverErr) {
		return fmt.Errorf("redis error reply: %w", err)
	}
	return err
}

// The whole reply is counted before limit cuts it since the server already sent all of it
// counted adds the replies of the commands that bounded it
func redisReply(reply any, q Query, counted float64, emit Emit) error {
	v, err := redisValue(reply)
	if err != nil {
		return err
	}
	used := q.use(redisCount(v) + counted)
	for _, rec := range redisRecords(v, q.Limit) {
		if err := emit(rec); err != nil {
			return err
		}
	}
	return used
}

// Every value in a reply
// 1. Each element of an array and each value of a map counts by what it holds
// 2. Any other value counts 1 including a nil reply
func redisCount(v any) float64 {
	n := 0.0
	switch x := v.(type) {
	case []any:
		for _, e := range x {
			n += redisCount(e)
		}
	case map[string]any:
		for _, e := range x {
			n += redisCount(e)
		}
	default:
		n = 1
	}
	return n
}

// Records per reply shape
// 1. map → one record with its keys sorted
// 2. array → index and value per element
// 3. single value → one value
// Records past limit are never built
func redisRecords(v any, limit int) []record.Record {
	if limit < 1 {
		return nil
	}
	switch x := v.(type) {
	case map[string]any:
		keys := slices.Sorted(maps.Keys(x))
		values := make([]any, len(keys))
		for i, k := range keys {
			values[i] = x[k]
		}
		return []record.Record{record.New(keys, values)}
	case []any:
		out := make([]record.Record, min(len(x), limit))
		for i := range out {
			out[i] = record.New([]string{"index", "value"}, []any{int64(i), x[i]})
		}
		return out
	}
	return []record.Record{record.New([]string{"value"}, []any{v})}
}

// A copy of the reply with string map keys so it encodes as JSON
// An error reply inside an array also ends in an error
func redisValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int64, float64:
		return x, nil
	case error:
		return nil, fmt.Errorf("redis error reply: %w", x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			c, err := redisValue(e)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			c, err := redisValue(e)
			if err != nil {
				return nil, err
			}
			out[fmt.Sprint(k)] = c
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			c, err := redisValue(e)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	}
	return fmt.Sprint(v), nil
}

// Splits on spaces like a shell and keeps spaces inside quotes
// 1. Single quotes keep everything as is
// 2. Backslash escapes work inside double quotes and outside quotes
func splitWords(s string) ([]string, error) {
	var words []string
	var cur strings.Builder
	inWord := false
	var quote rune
	escape := false
	for _, r := range s {
		switch {
		case escape:
			cur.WriteRune(r)
			escape = false
		case quote == '\'':
			if r == '\'' {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\\':
			escape = true
			inWord = true
		case quote == '"':
			if r == '"' {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			inWord = true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			if inWord {
				words = append(words, cur.String())
				cur.Reset()
				inWord = false
			}
		default:
			cur.WriteRune(r)
			inWord = true
		}
	}
	if quote != 0 || escape {
		return nil, errors.New("unclosed quote")
	}
	if inWord {
		words = append(words, cur.String())
	}
	return words, nil
}
