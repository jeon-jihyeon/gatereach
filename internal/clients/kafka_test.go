package clients

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// An in-memory Kafka cluster and a client connected to it
func kafkaCluster(t *testing.T, topics ...string) (*kafkaClient, string) {
	t.Helper()
	cluster := fakeCluster(t, topics...)
	addr := cluster.ListenAddrs()[0]
	return kafkaConnect(t, addr), addr
}

func fakeCluster(t *testing.T, topics ...string) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster
}

func kafkaConnect(t *testing.T, addr string) *kafkaClient {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	k := newKafka(config.Conn{Client: "kafka", Host: host, Port: p}, "").(*kafkaClient)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, k.Connect(ctx))
	t.Cleanup(func() { _ = k.Close() })
	return k
}

// Produces n records per partition
// transactional commits them in a transaction so a commit marker ends the log
func produce(t *testing.T, addr, topic string, n int, transactional bool) {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
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
		// How many times the same client reads
		reads int
		want  want
	}{
		{"samples each partition up to the limit", 3, false, "events", 2, 1, want{rows: 2}},
		{"reads everything under the limit", 3, false, "events", 100, 1, want{rows: 6}},
		{"the held client reads the same window again", 2, false, "events", 100, 2, want{rows: 4}},
		{"ends past a transaction commit marker", 3, true, "events", 100, 1, want{rows: 6}},
		{"empty topic ends at once", 0, false, "events", 100, 1, want{rows: 0}},
		{"topics lists metadata", 0, false, "topics", 100, 1, want{rows: 1}},
		{"missing topic fails", 0, false, "missing", 100, 1, want{err: "no such kafka topic: missing"}},
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
				assert.Equal(t, tc.want, want{rows: len(got), err: errText(err)})
			}
		})
	}
}

// A compacted topic can end in a gap with no record left at its end
// The broker here reports an end past the last record to make that gap
// 1. The idle wait ends the sample with what it read
// 2. A context that ends first returns its error so Run reports a timeout instead of a full sample
func TestKafkaReadEndsInAGap(t *testing.T) {
	t.Parallel()
	type want struct {
		rows int
		err  error
	}
	tcs := []struct {
		name string
		idle time.Duration
		// How long the read's context lasts
		wait time.Duration
		want want
	}{
		{"the idle wait ends the sample", 200 * time.Millisecond, 5 * time.Second, want{rows: 6}},
		{"a context that ends first keeps its error", time.Minute, time.Second,
			want{rows: 6, err: context.DeadlineExceeded}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k := kafkaGap(t)
			k.idle = tc.idle
			ctx, cancel := context.WithTimeout(context.Background(), tc.wait)
			defer cancel()
			rows := 0
			err := k.Read(ctx, Query{Text: "events", Limit: 100}, func(record.Record) error {
				rows++
				return nil
			})
			assert.Equal(t, tc.want, want{rows: rows, err: err})
		})
	}
}

// A client on a cluster whose 3 records per partition end before the reported end of 5
func kafkaGap(t *testing.T) *kafkaClient {
	t.Helper()
	cluster := fakeCluster(t, "events")
	addr := cluster.ListenAddrs()[0]
	produce(t, addr, "events", 3, false)
	cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ListOffsetsRequest)
		if req.Topics[0].Partitions[0].Timestamp != -1 {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.Offset = 5
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	return kafkaConnect(t, addr)
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

// Follow in the shape of Read so a table can hold both
func kafkaFollow(k *kafkaClient, ctx context.Context, q Query, emit Emit) error {
	_, err := k.Follow(ctx, q, emit)
	return err
}

// Each emitted record pays 1 and the record that reaches the budget is still emitted
func TestKafkaUse(t *testing.T) {
	t.Parallel()
	type want struct {
		used    []float64
		emitted int
		err     error
	}
	tcs := []struct {
		name  string
		query string
		read  func(k *kafkaClient, ctx context.Context, q Query, emit Emit) error
		// The total at which Use returns ErrBudget
		max  float64
		want want
	}{
		{"a sample pays per record", "events", (*kafkaClient).Read, 100, want{used: []float64{1, 1, 1, 1}, emitted: 4}},
		{"a sample stops at the budget", "events", (*kafkaClient).Read, 3,
			want{used: []float64{1, 1, 1}, emitted: 3, err: ErrBudget}},
		{"topics pays per row", "topics", (*kafkaClient).Read, 100, want{used: []float64{1}, emitted: 1}},
		{"a follow ends at the budget", "events", kafkaFollow, 2, want{used: []float64{1, 1}, emitted: 2, err: ErrBudget}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, addr := kafkaCluster(t, "events")
			produce(t, addr, "events", 2, false)
			var used []float64
			total := 0.0
			q := Query{Text: tc.query, Limit: 10, Use: func(amount float64) error {
				used = append(used, amount)
				if total += amount; total >= tc.max {
					return ErrBudget
				}
				return nil
			}}
			got, err := collect(t, func(ctx context.Context, emit Emit) error { return tc.read(k, ctx, q, emit) })
			assert.Equal(t, tc.want, want{used: used, emitted: len(got), err: err})
		})
	}
}

// An emit that fails ends the read at that record
func TestKafkaEmitStops(t *testing.T) {
	t.Parallel()
	emitErr := errors.New("sink closed")
	tcs := []struct {
		name  string
		query string
		read  func(k *kafkaClient, ctx context.Context, q Query, emit Emit) error
	}{
		{"a sample", "events", (*kafkaClient).Read},
		{"topics", "topics", (*kafkaClient).Read},
		{"a follow", "events", kafkaFollow},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, addr := kafkaCluster(t, "events")
			produce(t, addr, "events", 2, false)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			emitted := 0
			err := tc.read(k, ctx, Query{Text: tc.query, Limit: 10}, func(record.Record) error {
				emitted++
				return emitErr
			})
			assert.ErrorIs(t, err, emitErr)
			assert.Equal(t, 1, emitted)
		})
	}
}

// A binary payload becomes base64 instead of being lost to replacement characters
func TestKafkaRecord(t *testing.T) {
	t.Parallel()
	ts := time.UnixMilli(1700000000000)
	tcs := []struct {
		name string
		msg  *kgo.Record
		want string
	}{
		{"text stays a string", &kgo.Record{Topic: "t", Key: []byte("k"), Value: []byte("café"), Timestamp: ts,
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}}},
			`{"topic":"t","partition":0,"offset":0,"timestamp":1700000000000,"key":"k","value":"café","headers":[{"key":"h","value":"v"}]}`},
		{
			"binary becomes base64",
			&kgo.Record{Topic: "t", Key: []byte{0xff}, Value: []byte{0x00, 0xfe, 0x01}, Timestamp: ts,
				Headers: []kgo.RecordHeader{{Key: "h", Value: []byte{0xc3}}}},
			`{"topic":"t","partition":0,"offset":0,"timestamp":1700000000000,"key":"/w==","value":"AP4B","headers":[{"key":"h","value":"ww=="}]}`,
		},
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
