package clients

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 인메모리 Kafka 클러스터와 그 위의 연결된 클라이언트
func kafkaCluster(t *testing.T, topics ...string) (*kafkaClient, string) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	addr := cluster.ListenAddrs()[0]
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	k := newKafka(config.Conn{Client: "kafka", Host: host, Port: p}, "").(*kafkaClient)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, k.Connect(ctx))
	t.Cleanup(func() { _ = k.Close() })
	return k, addr
}

// 파티션마다 n건 생산
// transactional이면 트랜잭션으로 커밋해 커밋 마커가 끝에 남음
func produce(t *testing.T, addr, topic string, n int, transactional bool) {
	t.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(addr), kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	if transactional {
		opts = append(opts, kgo.TransactionalID("tx-"+topic))
	}
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if transactional {
		require.NoError(t, cl.BeginTransaction())
	}
	for p := int32(0); p < 2; p++ {
		for i := range n {
			r := &kgo.Record{Partition: p, Key: []byte("k"), Value: []byte(strconv.Itoa(i))}
			require.NoError(t, cl.ProduceSync(ctx, r).FirstErr())
		}
	}
	if transactional {
		require.NoError(t, cl.EndTransaction(ctx, kgo.TryCommit))
	}
}

func collect(t *testing.T, read func(context.Context, Emit) error) ([]record.Record, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []record.Record
	err := read(ctx, func(r record.Record) error {
		got = append(got, r)
		return nil
	})
	require.NoError(t, ctx.Err(), "read must end on its own")
	return got, err
}

func TestKafkaRead(t *testing.T) {
	t.Parallel()
	type want struct {
		rows int
		err  string
	}
	tcs := []struct {
		name          string
		perPartition  int
		transactional bool
		query         string
		limit         int
		// 같은 클라이언트로 반복해 읽는 횟수
		reads int
		want  want
	}{
		{"samples each partition up to the limit", 3, false, "events", 2, 1, want{rows: 2}},
		{"reads everything under the limit", 3, false, "events", 100, 1, want{rows: 6}},
		{"the held client reads the same window again", 2, false, "events", 100, 2, want{rows: 4}},
		{"ends past a transaction commit marker", 3, true, "events", 100, 1, want{rows: 6}},
		{"empty topic ends at once", 0, false, "events", 100, 1, want{rows: 0}},
		{"topics lists metadata", 0, false, "topics", 100, 1, want{rows: 1}},
		{"missing topic fails", 0, false, "missing", 100, 1, want{err: "missing"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, addr := kafkaCluster(t, "events")
			if tc.perPartition > 0 {
				produce(t, addr, "events", tc.perPartition, tc.transactional)
			}
			for range tc.reads {
				got, err := collect(t, func(ctx context.Context, emit Emit) error {
					return k.Read(ctx, Query{Text: tc.query, Limit: tc.limit}, emit)
				})
				if tc.want.err != "" {
					assert.ErrorContains(t, err, tc.want.err)
					return
				}
				require.NoError(t, err)
				assert.Len(t, got, tc.want.rows)
			}
		})
	}
}

func TestKafkaFollow(t *testing.T) {
	t.Parallel()
	k, addr := kafkaCluster(t, "events")
	produce(t, addr, "events", 1, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := 0
	handled, err := k.Follow(ctx, Query{Text: "events", Limit: 10}, func(r record.Record) error {
		got++
		if got == 2 {
			produce(t, addr, "events", 1, false)
		}
		if got == 4 {
			cancel()
		}
		return nil
	})
	assert.True(t, handled)
	assert.NoError(t, err)
	assert.Equal(t, 4, got, "recent records then new ones")

	handled, _ = k.Follow(context.Background(), Query{Text: "topics"}, nil)
	assert.False(t, handled, "topics falls back to polling")
}

// 바이너리 payload는 대체 문자로 잃지 않고 base64
func TestKafkaRecord(t *testing.T) {
	t.Parallel()
	ts := time.UnixMilli(1700000000000)
	tcs := []struct {
		name string
		msg  *kgo.Record
		want string
	}{
		{"text stays a string", &kgo.Record{Topic: "t", Key: []byte("k"), Value: []byte("값"), Timestamp: ts,
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}}},
			`{"topic":"t","partition":0,"offset":0,"timestamp":1700000000000,"key":"k","value":"값","headers":[{"key":"h","value":"v"}]}`},
		{"binary becomes base64", &kgo.Record{Topic: "t", Key: []byte{0xff}, Value: []byte{0x00, 0xfe, 0x01}, Timestamp: ts,
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte{0xc3}}}},
			`{"topic":"t","partition":0,"offset":0,"timestamp":1700000000000,"key":"/w==","value":"AP4B","headers":[{"key":"h","value":"ww=="}]}`},
		{"missing key and value are null", &kgo.Record{Topic: "t", Timestamp: ts},
			`{"topic":"t","partition":0,"offset":0,"timestamp":1700000000000,"key":null,"value":null,"headers":[]}`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, string(kafkaRecord(tc.msg).JSON()))
		})
	}
}
