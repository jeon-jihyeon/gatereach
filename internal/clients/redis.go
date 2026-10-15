package clients

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 읽기 전용 모드에서 허용하는 명령
// KEYS는 서버를 막을 수 있어 제외하고 SCAN 사용
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
		return errors.New("빈 명령")
	}
	if !write && !redisRead[strings.ToUpper(words[0])] {
		return fmt.Errorf("읽기 전용 모드에서 막힌 명령: %s\n쓰기가 필요하면 --write", words[0])
	}
	return nil
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
		// 단건 조회는 세션이 차례로 보내므로 연결 하나로 충분
		PoolSize: 1,
		// 프록시가 모르는 CLIENT SETINFO를 보내지 않음
		DisableIdentity: true,
		// 보낸 뒤의 EOF도 다시 보내는 드라이버 재시도를 꺼 `--write` 명령이 두 번 적용되지 않음
		// 쉬는 동안 끊긴 연결은 풀의 상태 검사가 다시 맺음
		MaxRetries: -1,
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

// limit은 응답을 받은 뒤 자르므로 서버 부하와 전송량은 명령 자체가 결정
func (r *redisClient) Read(ctx context.Context, q Query, emit Emit) error {
	if r.rdb == nil {
		return ErrDisconnected
	}
	words, err := splitWords(q.Text)
	if err != nil {
		return err
	}
	args := make([]any, len(words))
	for i, w := range words {
		args[i] = w
	}
	reply, err := r.rdb.Do(ctx, args...).Result()
	if errors.Is(err, redis.Nil) {
		reply, err = nil, nil
	}
	if err != nil {
		return fmt.Errorf("redis 오류 응답: %w", err)
	}
	v, err := redisValue(reply)
	if err != nil {
		return err
	}
	for i, rec := range redisRecords(v) {
		if i >= q.Limit {
			break
		}
		if err := emit(rec); err != nil {
			return err
		}
	}
	return nil
}

// 응답 모양별 레코드
// 1. map → 키 순서로 정렬한 레코드 하나
// 2. 배열 → 원소마다 index와 value
// 3. 단일 값 → value 하나
func redisRecords(v any) []record.Record {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		values := make([]any, len(keys))
		for i, k := range keys {
			values[i] = x[k]
		}
		return []record.Record{record.New(keys, values)}
	case []any:
		out := make([]record.Record, len(x))
		for i, e := range x {
			out[i] = record.New([]string{"index", "value"}, []any{int64(i), e})
		}
		return out
	}
	return []record.Record{record.New([]string{"value"}, []any{v})}
}

// RESP3 값을 레코드 값 타입으로
// 배열 안의 오류 응답도 오류로 끝냄
func redisValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int64, float64:
		return x, nil
	case error:
		return nil, fmt.Errorf("redis 오류 응답: %w", x)
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

// 셸처럼 공백으로 나누고 따옴표 안 공백은 유지
// 작은따옴표 안은 그대로 둠
// 큰따옴표 안과 밖은 백슬래시 이스케이프 처리
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
		return nil, errors.New("닫히지 않은 따옴표")
	}
	if inWord {
		words = append(words, cur.String())
	}
	return words, nil
}
