package clients_test

import (
	"context"
	"errors"
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
		{"mysql needs port and user", config.Conn{Client: "mysql"}, "mysql 연결 설정에 없는 키: port, user"},
		{"redis needs a port", config.Conn{Client: "redis"}, "redis 연결 설정에 없는 키: port"},
		{"dynamodb needs nothing", config.Conn{Client: "dynamodb"}, ""},
		{"kafka secret needs a user", config.Conn{Client: "kafka", Port: 1, Secret: "env:X"}, "kafka secret을 쓰려면 user 필요"},
		{"unknown client", config.Conn{Client: "mongo"}, "지원하지 않는 client: mongo"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := clients.Get(tc.conn)
			if tc.err == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tc.err)
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
		{"dynamodb allows SELECT", args{"dynamodb", "  select * from t", false}, true},
		{"dynamodb blocks DELETE", args{"dynamodb", "DELETE FROM t WHERE id = 1", false}, false},
		{"dynamodb allows DELETE with --write", args{"dynamodb", "DELETE FROM t", true}, true},
		{"kafka reads", args{"kafka", "events", false}, true},
		{"kafka never writes", args{"kafka", "events", true}, false},
		{"mysql takes one statement", args{"mysql", "DELETE FROM t", false}, true},
		{"mysql refuses a second statement", args{"mysql", "START TRANSACTION READ WRITE; DELETE FROM t", false}, false},
		{"mysql refuses a second statement with --write", args{"mysql", "select 1; select 2", true}, false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := clients.Kinds[tc.args.client].Check(tc.args.query, tc.args.write)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func rec(id int64, v string) record.Record {
	return record.New([]string{"id", "v"}, []any{id, v})
}

func TestWatch(t *testing.T) {
	t.Parallel()
	type want struct {
		emitted int
		err     bool
	}
	tcs := []struct {
		name string
		key  string
		runs [][]record.Record
		fail bool
		want want
	}{
		{"emits only unseen records", "", [][]record.Record{{rec(1, "a")}, {rec(1, "a"), rec(2, "b")}, {rec(2, "b")}}, false, want{emitted: 2}},
		{"key dedupes changed rows", "id", [][]record.Record{{rec(1, "a")}, {rec(1, "changed")}}, false, want{emitted: 1}},
		{"missing key falls back to the whole record", "nope", [][]record.Record{{rec(1, "a")}, {rec(1, "changed")}}, false, want{emitted: 2}},
		{"a read error ends the watch", "", [][]record.Record{{rec(1, "a")}}, true, want{emitted: 1, err: true}},
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
						if last && !tc.fail {
							cancel()
						}
						return nil
					}))
			}
			if tc.fail {
				calls = append(calls, c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("끊김")))
			}
			gomock.InOrder(calls...)
			emitted := 0
			err := clients.Watch(ctx, c, clients.Query{Limit: 10}, clients.Watching{Interval: time.Millisecond, Key: tc.key}, func(record.Record) error {
				emitted++
				return nil
			})
			assert.Equal(t, tc.want.err, err != nil, "%v", err)
			assert.Equal(t, tc.want.emitted, emitted)
		})
	}
}
