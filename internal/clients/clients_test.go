package clients_test

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/clients/mocks"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

func TestGet(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		conn config.Conn
		err  string
	}{
		{"mysql needs port and user", config.Conn{Client: "mysql"}, "mysql connection is missing: port, user"},
		{"redis needs a port", config.Conn{Client: "redis"}, "redis connection is missing: port"},
		{"dynamodb needs nothing", config.Conn{Client: "dynamodb"}, ""},
		{"kafka secret needs a user", config.Conn{Client: "kafka", Port: 1, Secret: "env:X"}, "a kafka secret needs a user"},
		{"unknown client", config.Conn{Client: "mongo"},
			"unsupported client: mongo\navailable: athena dynamodb kafka mysql redis"},
		{"athena needs a workgroup", config.Conn{Client: "athena", Region: "us-east-1"},
			"athena connection is missing: workgroup"},
		{
			"athena output location must be an s3 URL",
			config.Conn{Client: "athena", Workgroup: "primary", OutputLocation: "https://example.com/r"},
			"athena output_location must start with s3://: https://example.com/r",
		},
		{"athena with a workgroup and an s3 output location",
			config.Conn{Client: "athena", Workgroup: "primary", OutputLocation: "s3://example-bucket/r/"}, ""},
		{"athena output location is optional", config.Conn{Client: "athena", Workgroup: "primary"}, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := clients.Get(tc.conn)
			assert.Equal(t, tc.err, errText(err))
		})
	}
}

// Every client refuses a read before Connect so the session reconnects instead of panicking
func TestReadBeforeConnect(t *testing.T) {
	t.Parallel()
	for _, name := range clients.Names() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := clients.Kinds[name].New(config.Conn{Client: name}, "")
			err := c.Read(
				context.Background(),
				clients.Query{Text: "x", Limit: 1},
				func(record.Record) error { return nil },
			)
			assert.ErrorIs(t, err, clients.ErrDisconnected)
			assert.NoError(t, c.Close())
		})
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()
	type args struct {
		client, query string
		write         bool
	}
	tcs := []struct {
		name string
		args args
		ok   bool
	}{
		{"redis allows reads", args{"redis", "HGETALL user:1", false}, true},
		{"redis blocks writes", args{"redis", "SET k v", false}, false},
		{"redis blocks KEYS", args{"redis", "keys *", false}, false},
		{"redis allows writes with --write", args{"redis", "SET k v", true}, true},
		{"redis refuses an empty command", args{"redis", "  ", true}, false},
		{"redis refuses an open quote", args{"redis", `GET "k`, false}, false},
		{"redis allows SCAN COUNT up to 1000", args{"redis", "SCAN 0 MATCH user:* COUNT 1000", false}, true},
		{"redis refuses SCAN COUNT past 1000", args{"redis", "scan 0 count 1001", false}, false},
		{"redis refuses HSCAN COUNT past 1000", args{"redis", "HSCAN h 0 COUNT 5000", false}, false},
		{"redis reads a MATCH pattern named COUNT as a pattern", args{"redis", "SSCAN s 0 MATCH COUNT", false}, true},
		{"redis allows any SCAN COUNT with --write", args{"redis", "SCAN 0 COUNT 100000", true}, true},
		{"dynamodb allows SELECT", args{"dynamodb", "  select * from t", false}, true},
		{"dynamodb blocks DELETE", args{"dynamodb", "DELETE FROM t WHERE id = 1", false}, false},
		{"dynamodb allows DELETE with --write", args{"dynamodb", "DELETE FROM t", true}, true},
		{"kafka reads", args{"kafka", "events", false}, true},
		{"kafka never writes", args{"kafka", "events", true}, false},
		{"mysql refuses DELETE without --write", args{"mysql", "DELETE FROM t", false}, false},
		{"mysql takes one statement with --write", args{"mysql", "DELETE FROM t", true}, true},
		{"mysql refuses sleep", args{"mysql", "SELECT SLEEP(10)", false}, false},
		{"mysql refuses a second statement", args{"mysql", "START TRANSACTION READ WRITE; DELETE FROM t", false}, false},
		{"mysql refuses a second statement with --write", args{"mysql", "select 1; select 2", true}, false},
		{"athena allows SELECT", args{"athena", "  select * from t", false}, true},
		{"athena allows WITH", args{"athena", "WITH x AS (SELECT 1) SELECT * FROM x", false}, true},
		{"athena allows SHOW", args{"athena", "show tables", false}, true},
		{"athena allows DESCRIBE", args{"athena", "DESCRIBE t", false}, true},
		{"athena allows EXPLAIN", args{"athena", "EXPLAIN INSERT INTO t SELECT 1", false}, true},
		{"athena allows EXPLAIN ANALYZE of a read", args{"athena", "explain analyze verbose select 1", false}, true},
		{"athena blocks EXPLAIN ANALYZE of a write", args{"athena", "EXPLAIN ANALYZE INSERT INTO t SELECT 1", false}, false},
		{"athena skips comments and parentheses", args{"athena", "-- note\n/* why */ (SELECT 1)", false}, true},
		{"athena ends a line comment at CR", args{"athena", "EXPLAIN -- note\r ANALYZE INSERT INTO t SELECT 1", false},
			false},
		{"athena blocks a word that starts like SELECT", args{"athena", "SELECTX 1", false}, false},
		{"athena blocks a word that ends in a digit", args{"athena", "SELECT1", false}, false},
		{"athena allows EXPLAIN ANALYZE with a format", args{"athena", "EXPLAIN ANALYZE (FORMAT JSON) SELECT 1", false},
			true},
		{"athena blocks EXPLAIN ANALYZE with a format of a write",
			args{"athena", "EXPLAIN ANALYZE (FORMAT JSON) INSERT INTO t SELECT 1", false}, false},
		{"athena blocks INSERT", args{"athena", "INSERT INTO t VALUES (1)", false}, false},
		{"athena blocks UNLOAD", args{"athena", "UNLOAD (SELECT 1) TO 's3://example-bucket/x/'", false}, false},
		{"athena blocks CREATE TABLE AS", args{"athena", "CREATE TABLE t AS SELECT 1", false}, false},
		{"athena allows EXPLAIN ANALYZE of a WITH query",
			args{"athena", "EXPLAIN ANALYZE WITH x AS (SELECT 1) SELECT * FROM x", false}, true},
		{"athena blocks an empty query", args{"athena", " -- only a comment", false}, false},
		{"athena blocks an unclosed comment", args{"athena", "/* SELECT 1", false}, false},
		{"athena allows writes with --write", args{"athena", "DROP TABLE t", true}, true},
		{"athena refuses system.query",
			args{"athena", "SELECT * FROM TABLE(acme.system.query(query => 'SELECT 1'))", false}, false},
		{"athena refuses system.query in any case and spacing",
			args{"athena", "SELECT * FROM TABLE(System . Query(q))", false}, false},
		{"athena refuses system.query across a comment", args{"athena", "SELECT * FROM TABLE(system/* x */.query(q))", false},
			false},
		{"athena refuses a quoted system.query", args{"athena", `SELECT * FROM TABLE("system"."query"(q))`, false}, false},
		{"athena allows system.query in a string", args{"athena", "SELECT 'system.query' AS s", false}, true},
		{"athena allows system.query in a comment", args{"athena", "SELECT 1 -- system.query", false}, true},
		{"athena allows a column named query", args{"athena", "SELECT s.query FROM system_log s", false}, true},
		{"athena allows system.query with --write", args{"athena", "SELECT * FROM TABLE(system.query(q))", true}, true},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := clients.Kinds[tc.args.client].Check(tc.args.query, tc.args.write)
			assert.Equal(t, tc.ok, err == nil, errText(err))
		})
	}
}

func TestBudgetParse(t *testing.T) {
	t.Parallel()
	type want struct {
		amount float64
		err    bool
	}
	tcs := []struct {
		name   string
		client string
		text   string
		want   want
	}{
		{"athena plain bytes", "athena", "1500", want{amount: 1500}},
		{"athena KB", "athena", "2KB", want{amount: 2e3}},
		{"athena MB", "athena", "3MB", want{amount: 3e6}},
		{"athena GB in powers of 1000", "athena", "10GB", want{amount: 10e9}},
		{"athena TB", "athena", "1TB", want{amount: 1e12}},
		{"athena fraction with a suffix", "athena", "1.5GB", want{amount: 1.5e9}},
		{"athena lower case and a space", "athena", "10 gb", want{amount: 10e9}},
		{"athena refuses a fraction of a byte", "athena", "1.5", want{err: true}},
		{"athena refuses an unknown suffix", "athena", "10XB", want{err: true}},
		{"athena refuses a bare suffix", "athena", "GB", want{err: true}},
		{"athena refuses zero", "athena", "0GB", want{err: true}},
		{"athena refuses a negative", "athena", "-1GB", want{err: true}},
		{"athena refuses a value past float64 once scaled", "athena", "1" + strings.Repeat("0", 300) + "TB", want{err: true}},
		{"dynamodb fraction", "dynamodb", "2.5", want{amount: 2.5}},
		{"dynamodb whole", "dynamodb", "10", want{amount: 10}},
		{"dynamodb refuses zero", "dynamodb", "0", want{err: true}},
		{"dynamodb refuses a negative", "dynamodb", "-2", want{err: true}},
		{"dynamodb refuses an exponent", "dynamodb", "1e3", want{err: true}},
		{"dynamodb refuses a trailing dot", "dynamodb", "2.", want{err: true}},
		{"dynamodb refuses text", "dynamodb", "NaN", want{err: true}},
		{"mysql whole", "mysql", "100000", want{amount: 100000}},
		{"mysql refuses a fraction", "mysql", "1.5", want{err: true}},
		{"mysql refuses zero", "mysql", "0", want{err: true}},
		{"redis whole", "redis", "1000", want{amount: 1000}},
		{"redis refuses a sign", "redis", "+5", want{err: true}},
		{"redis refuses empty", "redis", "", want{err: true}},
		{"kafka whole", "kafka", "50", want{amount: 50}},
		{"kafka refuses a suffix", "kafka", "50k", want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			amount, err := clients.Kinds[tc.client].Budget.Parse(tc.text)
			assert.Equal(t, tc.want, want{amount: amount, err: err != nil})
		})
	}
}

func TestBudgetFormat(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name   string
		client string
		amount float64
		want   string
	}{
		{"athena bytes", "athena", 512, "512 bytes"},
		{"athena one decimal in GB", "athena", 1.23e9, "1.2 GB"},
		{"athena drops a zero decimal", "athena", 10e9, "10 GB"},
		{"athena KB", "athena", 1500, "1.5 KB"},
		{"athena TB", "athena", 2e12, "2 TB"},
		{"athena rounding up to 1000 moves to the next unit", "athena", 999.96e9, "1 TB"},
		{"athena rounding up to 1000 KB moves to MB", "athena", 999_960, "1 MB"},
		{"athena just under a rounding step stays", "athena", 999.94e9, "999.9 GB"},
		{"dynamodb fraction", "dynamodb", 0.1 + 0.2, "0.3 read capacity units"},
		{"mysql", "mysql", 1200, "1200 rows examined"},
		{"redis", "redis", 3, "3 reply elements"},
		{"one of a count", "kafka", 1, "1 record"},
		{"kafka", "kafka", 0, "0 records"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clients.Kinds[tc.client].Budget.Format(tc.amount))
		})
	}
}

// A Read that waits for its context and returns the context's error
func blockRead(ctx context.Context, _ clients.Query, _ clients.Emit) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRun(t *testing.T) {
	t.Parallel()
	readErr := errors.New("dropped")
	tcs := []struct {
		name    string
		timeout time.Duration
		// Ends the caller's context before the run's own deadline
		cancel bool
		read   func(context.Context, clients.Query, clients.Emit) error
		err    string
	}{
		{"a run past the timeout", 10 * time.Millisecond, false, blockRead, "query ran past --timeout 10ms"},
		{"a socket deadline past the timeout", 10 * time.Millisecond, false,
			func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
				<-ctx.Done()
				return &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
			}, "query ran past --timeout 10ms"},
		{"another error past the timeout keeps its message", 10 * time.Millisecond, false,
			func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
				<-ctx.Done()
				return readErr
			}, "dropped"},
		{"a cancelled request keeps its error", time.Minute, true, blockRead, context.Canceled.Error()},
		{"no timeout leaves the read alone", 0, false,
			func(context.Context, clients.Query, clients.Emit) error { return readErr }, "dropped"},
		{"a driver error inside the timeout stays", time.Minute, false,
			func(context.Context, clients.Query, clients.Emit) error { return readErr }, "dropped"},
		{"a budget inside the timeout stays", time.Minute, false,
			func(context.Context, clients.Query, clients.Emit) error { return clients.ErrBudget }, clients.ErrBudget.Error()},
		{"a read inside the timeout ends cleanly", time.Minute, false,
			func(context.Context, clients.Query, clients.Emit) error { return nil }, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			c := mocks.NewMockClient(gomock.NewController(t))
			c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(tc.read)
			err := clients.Run(
				ctx,
				c,
				clients.Query{Limit: 1, Timeout: tc.timeout},
				func(record.Record) error { return nil },
			)
			assert.Equal(t, tc.err, errText(err))
		})
	}
}

// The error text or empty so a table compares outcomes in one assert
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func rec(id int64, v string) record.Record {
	return record.New([]string{"id", "v"}, []any{id, v})
}

func TestWatch(t *testing.T) {
	t.Parallel()
	dropped := errors.New("dropped")
	type want struct {
		emitted int
		err     error
	}
	tcs := []struct {
		name string
		key  string
		runs [][]record.Record
		// What one more run returns after emitting a new record
		// nil ends the watch by cancel after the runs
		end  error
		want want
	}{
		{"emits only unseen records", "", [][]record.Record{{rec(1, "a")}, {rec(1, "a"), rec(2, "b")}, {rec(2, "b")}}, nil,
			want{emitted: 2}},
		{"key dedupes changed rows", "id", [][]record.Record{{rec(1, "a")}, {rec(1, "changed")}}, nil, want{emitted: 1}},
		{"missing key falls back to the whole record", "nope", [][]record.Record{{rec(1, "a")}, {rec(1, "changed")}}, nil,
			want{emitted: 2}},
		{"a read error ends the watch", "", [][]record.Record{{rec(1, "a")}}, dropped, want{emitted: 2, err: dropped}},
		{"a budget ends the watch after the paid record", "", [][]record.Record{{rec(1, "a")}}, clients.ErrBudget,
			want{emitted: 2, err: clients.ErrBudget}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := mocks.NewMockClient(gomock.NewController(t))
			calls := []any{}
			for i, run := range tc.runs {
				last := i == len(tc.runs)-1
				calls = append(calls, c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ clients.Query, emit clients.Emit) error {
						for _, r := range run {
							require.NoError(t, emit(r))
						}
						if last && tc.end == nil {
							cancel()
						}
						return nil
					}))
			}
			if tc.end != nil {
				calls = append(calls, c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ clients.Query, emit clients.Emit) error {
						require.NoError(t, emit(rec(9, "paid")))
						return tc.end
					}))
			}
			gomock.InOrder(calls...)
			emitted := 0
			err := clients.Watch(
				ctx,
				c,
				clients.Query{Limit: 10},
				clients.Watching{Interval: time.Millisecond, Key: tc.key},
				func(record.Record) error {
					emitted++
					return nil
				},
			)
			assert.ErrorIs(t, err, tc.want.err)
			assert.Equal(t, tc.want.emitted, emitted)
		})
	}
}

// A client whose source can push records
type followingClient struct {
	*mocks.MockClient
	*mocks.MockFollower
}

func TestWatchFollows(t *testing.T) {
	t.Parallel()
	followErr := errors.New("tail ended")
	type want struct {
		reads int
		err   error
	}
	tcs := []struct {
		name    string
		handled bool
		want    want
	}{
		{"a handled query keeps the follow's end", true, want{err: followErr}},
		{"an unhandled query is polled", false, want{reads: 1}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctrl := gomock.NewController(t)
			c := followingClient{mocks.NewMockClient(ctrl), mocks.NewMockFollower(ctrl)}
			c.MockFollower.EXPECT().Follow(gomock.Any(), gomock.Any(), gomock.Any()).Return(tc.handled, tc.want.err)
			reads := 0
			c.MockClient.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, clients.Query, clients.Emit) error {
					reads++
					cancel()
					return nil
				}).Times(tc.want.reads)
			err := clients.Watch(ctx, c, clients.Query{Limit: 1}, clients.Watching{Interval: time.Millisecond},
				func(record.Record) error { return nil })
			assert.Equal(t, tc.want, want{reads: reads, err: err})
		})
	}
}

// The CLI falls back to these when --timeout isn't given
func TestKindTimeout(t *testing.T) {
	t.Parallel()
	want := map[string]time.Duration{
		"mysql": 30 * time.Second, "redis": 10 * time.Second, "dynamodb": 60 * time.Second, "athena": 0, "kafka": 0,
	}
	got := map[string]time.Duration{}
	for name, k := range clients.Kinds {
		got[name] = k.Timeout
	}
	assert.Equal(t, want, got)
}
