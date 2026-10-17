package clients

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// A query that lists topic metadata instead of reading a topic
const kafkaTopics = "topics"

type kafkaClient struct {
	conn     config.Conn
	password string
	// The session holds one client
	// Each read only adds and drops partitions so the connection and auth are reused
	cl   *kgo.Client
	adm  *kadm.Client
	idle time.Duration
}

func newKafka(conn config.Conn, password string) Client {
	return &kafkaClient{conn: conn, password: password, idle: kafkaIdle}
}

// It only consumes so there's no write path
func kafkaCheck(_ string, write bool) error {
	if write {
		return errors.New("kafka only reads")
	}
	return nil
}

// SASL PLAIN needs the user and the secret together
func kafkaValidate(conn config.Conn) error {
	if conn.Secret != "" && conn.User == "" {
		return errors.New("a kafka secret needs a user")
	}
	return nil
}

// Partitions are read directly without a consumer group so no offset commit is left behind
// Commit markers of transactional topics are kept only to detect the end
func (k *kafkaClient) Connect(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(net.JoinHostPort(k.conn.Host, strconv.Itoa(k.conn.Port))),
		kgo.KeepControlRecords(),
	}
	if k.password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: k.conn.User, Pass: k.password}.AsMechanism()))
	}
	// SASL PLAIN sends the secret as is so it never goes without TLS
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
		return k.topics(ctx, q.Limit, q.use, emit)
	}
	return k.sample(ctx, topic, q.Limit, q.use, emit)
}

// topics is left to polling
// A topic keeps its consumer open and takes new messages
func (k *kafkaClient) Follow(ctx context.Context, q Query, emit Emit) (bool, error) {
	topic := strings.TrimSpace(q.Text)
	if topic == kafkaTopics {
		return false, nil
	}
	return true, k.tail(ctx, topic, q.Limit, q.use, emit)
}

// Each record is paid for once it's out so it's emitted before use can stop the read
func emitPaid(r record.Record, use func(float64) error, emit Emit) error {
	if err := emit(r); err != nil {
		return err
	}
	return use(1)
}

func (k *kafkaClient) topics(ctx context.Context, limit int, use func(float64) error, emit Emit) error {
	td, err := k.adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	// kadm returns the names sorted
	for i, n := range td.Names() {
		if i >= limit {
			break
		}
		r := record.New([]string{"topic", "partitions"}, []any{n, int64(len(td[n].Partitions))})
		if err := emitPaid(r, use, emit); err != nil {
			return err
		}
	}
	return nil
}

// Per partition where to read from and the end when the read started
// Reading starts limit records before the end
// The ends leave out an empty partition whose end isn't past its start
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
	err := errors.Join(serr, eerr, ends.Error())
	// A missing topic comes back as a listed partition -1 with this error
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return nil, nil, errors.New("no such kafka topic: " + topic)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("kafka topic %s lookup failed: %w", topic, err)
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
		return nil, nil, errors.New("no such kafka topic: " + topic)
	}
	return from, until, nil
}

// The returned func drops the partitions and any messages left in the buffer
func (k *kafkaClient) attach(topic string, from map[int32]kgo.Offset) func() {
	k.cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: from})
	return func() { k.cl.PurgeTopicsFromConsuming(topic) }
}

// How long a sample waits for a fetch before it takes every open partition as ended
// 1. A compacted topic can lose the records just before its end so no record arrives at the end itself
// 2. The client hides fetches that bring nothing so a partition caught up in such a gap goes quiet
// 3. A broker answers a fetch at once when it holds records past the position
const kafkaIdle = 10 * time.Second

// Stops at the end seen when the read started or after limit records in total
// With several partitions it's a per partition sample and not the topic's latest limit records
// A read cut by its context returns the context's error so Run can tell a timeout from a full sample
func (k *kafkaClient) sample(ctx context.Context, topic string, limit int, use func(float64) error, emit Emit) error {
	from, until, err := k.window(ctx, topic, limit)
	if err != nil || len(until) == 0 {
		return err
	}
	defer k.attach(topic, from)()
	n := 0
	for len(until) > 0 && n < limit {
		idle, cancel := context.WithTimeout(ctx, k.idle)
		fetches := k.cl.PollFetches(idle)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		// Nothing came within the idle wait so every open partition sits at its end
		if errors.Is(fetches.Err0(), context.DeadlineExceeded) {
			return nil
		}
		if err := fetchError(fetches); err != nil {
			return err
		}
		var stop error
		fetches.EachRecord(func(m *kgo.Record) {
			end, open := until[m.Partition]
			if stop != nil || n >= limit || !open {
				return
			}
			// A record past the end shows the position passed it over a gap
			if m.Offset >= end {
				delete(until, m.Partition)
				return
			}
			if m.Offset+1 >= end {
				delete(until, m.Partition)
			}
			if m.Attrs.IsControl() {
				return
			}
			if stop = emitPaid(kafkaRecord(m), use, emit); stop == nil {
				n++
			}
		})
		if stop != nil {
			return stop
		}
	}
	return nil
}

// Emits the latest limit records per partition first and then keeps taking new messages
// It has no runs so a timeout never applies
func (k *kafkaClient) tail(ctx context.Context, topic string, limit int, use func(float64) error, emit Emit) error {
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
				stop = emitPaid(kafkaRecord(m), use, emit)
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

// 1. UTF-8 text is a string
// 2. Other bytes are a base64 string like MySQL binary columns so nothing is lost to replacement characters
// 3. nil is null
func kafkaBytes(b []byte) any {
	if b == nil {
		return nil
	}
	if utf8.Valid(b) {
		return string(b)
	}
	return base64.StdEncoding.EncodeToString(b)
}
