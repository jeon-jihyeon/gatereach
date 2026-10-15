package clients

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 토픽 대신 메타데이터를 조회하는 쿼리
const kafkaTopics = "topics"

type kafkaClient struct {
	conn     config.Conn
	password string
	// 세션이 쥐는 클라이언트 하나
	// 조회마다 파티션만 붙였다 떼어 연결과 인증을 재사용
	cl  *kgo.Client
	adm *kadm.Client
}

func newKafka(conn config.Conn, password string) Client {
	return &kafkaClient{conn: conn, password: password}
}

// 소비만 하므로 쓰기 경로 없음
func kafkaCheck(_ string, write bool) error {
	if write {
		return errors.New("kafka는 읽기만 지원")
	}
	return nil
}

// SASL PLAIN은 계정과 비밀값이 함께 있어야 함
func kafkaValidate(conn config.Conn) error {
	if conn.Secret != "" && conn.User == "" {
		return errors.New("kafka secret을 쓰려면 user 필요")
	}
	return nil
}

// 컨슈머 그룹 없이 파티션을 직접 읽어 오프셋 커밋이 남지 않음
// 트랜잭션 토픽의 커밋 마커도 받아 끝 판정에만 씀
func (k *kafkaClient) Connect(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(net.JoinHostPort(k.conn.Host, strconv.Itoa(k.conn.Port))),
		kgo.KeepControlRecords(),
	}
	if k.password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: k.conn.User, Pass: k.password}.AsMechanism()))
	}
	if k.password != "" || k.conn.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return err
	}
	k.cl, k.adm = cl, kadm.NewClient(cl)
	return nil
}

func (k *kafkaClient) Close() error {
	if k.cl != nil {
		k.cl.Close()
	}
	return nil
}

func (k *kafkaClient) Read(ctx context.Context, q Query, emit Emit) error {
	if k.cl == nil {
		return ErrDisconnected
	}
	topic := strings.TrimSpace(q.Text)
	if topic == kafkaTopics {
		return k.topics(ctx, q.Limit, emit)
	}
	return k.sample(ctx, topic, q.Limit, emit)
}

// topics는 폴링에 맡기고 토픽은 컨슈머를 끝내지 않고 새 메시지를 받음
func (k *kafkaClient) Follow(ctx context.Context, q Query, emit Emit) (bool, error) {
	topic := strings.TrimSpace(q.Text)
	if topic == kafkaTopics {
		return false, nil
	}
	return true, k.tail(ctx, topic, q.Limit, emit)
}

func (k *kafkaClient) topics(ctx context.Context, limit int, emit Emit) error {
	td, err := k.adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	names := td.Names()
	sort.Strings(names)
	for i, n := range names {
		if i >= limit {
			break
		}
		r := record.New([]string{"topic", "partitions"}, []any{n, int64(len(td[n].Partitions))})
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// 파티션마다 끝에서 limit 건 앞부터 읽을 위치와 시작 시점의 끝
// 끝이 시작보다 크지 않은 빈 파티션은 ends에서 빠짐
func (k *kafkaClient) window(ctx context.Context, topic string, limit int) (map[int32]kgo.Offset, map[int32]int64, error) {
	var (
		wg           sync.WaitGroup
		starts, ends kadm.ListedOffsets
		serr, eerr   error
	)
	wg.Add(2)
	go func() { defer wg.Done(); starts, serr = k.adm.ListStartOffsets(ctx, topic) }()
	go func() { defer wg.Done(); ends, eerr = k.adm.ListEndOffsets(ctx, topic) }()
	wg.Wait()
	if err := errors.Join(serr, eerr); err != nil {
		return nil, nil, fmt.Errorf("kafka 토픽 %s 조회 실패: %w", topic, err)
	}
	if err := ends.Error(); err != nil {
		return nil, nil, fmt.Errorf("kafka 토픽 %s 조회 실패: %w", topic, err)
	}
	from := map[int32]kgo.Offset{}
	until := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		start := max(o.Offset-int64(limit), 0)
		if s, ok := starts.Lookup(topic, o.Partition); ok {
			start = max(start, s.Offset)
		}
		from[o.Partition] = kgo.NewOffset().At(start)
		if o.Offset > start {
			until[o.Partition] = o.Offset
		}
	})
	if len(from) == 0 {
		return nil, nil, errors.New("kafka 토픽 없음: " + topic)
	}
	return from, until, nil
}

// 붙인 파티션을 끝에 떼고 버퍼에 남은 메시지도 버림
func (k *kafkaClient) attach(topic string, from map[int32]kgo.Offset) func() {
	k.cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: from})
	return func() { k.cl.PurgeTopicsFromConsuming(topic) }
}

// 시작 시점의 끝까지 읽거나 합계 limit 건에서 종료
// 파티션이 여럿이면 토픽 전체의 최신 limit 건이 아닌 파티션별 표본
func (k *kafkaClient) sample(ctx context.Context, topic string, limit int, emit Emit) error {
	from, until, err := k.window(ctx, topic, limit)
	if err != nil || len(until) == 0 {
		return err
	}
	defer k.attach(topic, from)()
	n := 0
	for len(until) > 0 && n < limit {
		fetches := k.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetchError(fetches); err != nil {
			return err
		}
		var stop error
		fetches.EachRecord(func(m *kgo.Record) {
			end, open := until[m.Partition]
			if stop != nil || n >= limit || !open || m.Offset >= end {
				return
			}
			if m.Offset+1 >= end {
				delete(until, m.Partition)
			}
			if m.Attrs.IsControl() {
				return
			}
			if stop = emit(kafkaRecord(m)); stop == nil {
				n++
			}
		})
		if stop != nil {
			return stop
		}
	}
	return nil
}

// 파티션마다 최근 limit 건을 먼저 내보내고 이후 새 메시지를 계속 받음
func (k *kafkaClient) tail(ctx context.Context, topic string, limit int, emit Emit) error {
	from, _, err := k.window(ctx, topic, limit)
	if err != nil {
		return err
	}
	defer k.attach(topic, from)()
	for {
		fetches := k.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetchError(fetches); err != nil {
			return err
		}
		var stop error
		fetches.EachRecord(func(m *kgo.Record) {
			if stop == nil && !m.Attrs.IsControl() {
				stop = emit(kafkaRecord(m))
			}
		})
		if stop != nil {
			return stop
		}
	}
}

func fetchError(f kgo.Fetches) error {
	var err error
	f.EachError(func(_ string, _ int32, e error) {
		if err == nil && !errors.Is(e, context.Canceled) {
			err = e
		}
	})
	return err
}

func kafkaRecord(m *kgo.Record) record.Record {
	headers := make([]any, len(m.Headers))
	for i, h := range m.Headers {
		headers[i] = map[string]any{"key": h.Key, "value": kafkaBytes(h.Value)}
	}
	return record.New(
		[]string{"topic", "partition", "offset", "timestamp", "key", "value", "headers"},
		[]any{m.Topic, int64(m.Partition), m.Offset, m.Timestamp.UnixMilli(), kafkaBytes(m.Key), kafkaBytes(m.Value), headers},
	)
}

// UTF-8 텍스트는 문자열
// 그 밖의 바이트는 MySQL 바이너리 열처럼 base64 문자열이라 대체 문자로 잃지 않음
// 없으면 null
func kafkaBytes(b []byte) any {
	if b == nil {
		return nil
	}
	if utf8.Valid(b) {
		return string(b)
	}
	return base64.StdEncoding.EncodeToString(b)
}
