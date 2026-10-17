package clients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// What one ExecuteStatement request asked for
type dynamoCall struct {
	Limit                  int
	ReturnConsumedCapacity string
}

// An HTTP server that returns ExecuteStatement pages in order and records each request
// Every page consumes 0.5 read capacity units
func dynamoServer(t *testing.T, pages [][]int) (*dynamoClient, *[]dynamoCall) {
	t.Helper()
	var mu sync.Mutex
	var calls []dynamoCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in dynamoCall
		_ = json.Unmarshal(body, &in)
		mu.Lock()
		calls = append(calls, in)
		call := len(calls)
		mu.Unlock()
		page := pages[call-1]
		items := make([]map[string]any, len(page))
		for i, id := range page {
			items[i] = map[string]any{"id": map[string]string{"N": fmt.Sprint(id)}}
		}
		out := map[string]any{
			"Items":            items,
			"ConsumedCapacity": map[string]any{"TableName": "t", "CapacityUnits": 0.5},
		}
		if call < len(pages) {
			out["NextToken"] = fmt.Sprintf("t%d", call)
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return dynamoAt(srv.URL), &calls
}

func dynamoAt(url string) *dynamoClient {
	d := newDynamoDB(config.Conn{Client: "dynamodb"}, "").(*dynamoClient)
	d.db = dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(url),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
	})
	return d
}

// A server that answers every request with a ValidationException
func dynamoRefuses(t *testing.T) *dynamoClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"__type": "com.amazonaws.dynamodb.v20120810#ValidationException", "message": "bad statement",
		})
	}))
	t.Cleanup(srv.Close)
	return dynamoAt(srv.URL)
}

func TestDynamoReadEnds(t *testing.T) {
	t.Parallel()
	serves := func(t *testing.T) *dynamoClient {
		d, _ := dynamoServer(t, [][]int{{1, 2}, {3}})
		return d
	}
	type want struct {
		emitted int
		err     string
	}
	tcs := []struct {
		name    string
		client  func(t *testing.T) *dynamoClient
		emitErr error
		want    want
	}{
		{"an error reply ends the read", dynamoRefuses, nil, want{err: "ValidationException: bad statement"}},
		{"an emit that fails stops the stream", serves, errors.New("sink closed"), want{emitted: 1, err: "sink closed"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			emitted := 0
			err := tc.client(t).
				Read(context.Background(), Query{Text: "SELECT * FROM t", Limit: 10}, func(record.Record) error {
					emitted++
					return tc.emitErr
				})
			assert.ErrorContains(t, err, tc.want.err)
			assert.Equal(t, tc.want.emitted, emitted)
		})
	}
}

func TestDynamoRead(t *testing.T) {
	t.Parallel()
	type want struct {
		ids    []string
		limits []int
	}
	tcs := []struct {
		name  string
		pages [][]int
		limit int
		want  want
	}{
		{"follows the next token", [][]int{{1, 2}, {3}}, 10, want{ids: []string{"1", "2", "3"}, limits: []int{1000, 1000}}},
		{"stops mid page at the limit", [][]int{{1, 2, 3}}, 2, want{ids: []string{"1", "2"}, limits: []int{1000}}},
		{"asks for the limit when it is above the page minimum", [][]int{{1}}, 5000,
			want{ids: []string{"1"}, limits: []int{5000}}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, calls := dynamoServer(t, tc.pages)
			var ids []string
			err := d.Read(
				context.Background(),
				Query{Text: "SELECT * FROM t", Limit: tc.limit},
				func(r record.Record) error {
					v, _ := r.Get("id")
					ids = append(ids, v.(string))
					return nil
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want.ids, ids)
			var limits []int
			for _, c := range *calls {
				limits = append(limits, c.Limit)
				assert.Empty(t, c.ReturnConsumedCapacity, "capacity is asked for only with --max")
			}
			assert.Equal(t, tc.want.limits, limits)
		})
	}
}

func TestDynamoUse(t *testing.T) {
	t.Parallel()
	type want struct {
		used     []float64
		ids      []string
		requests int
		err      error
	}
	tcs := []struct {
		name  string
		pages [][]int
		limit int
		// The total at which Use returns ErrBudget
		max  float64
		want want
	}{
		{"reports each page", [][]int{{1, 2}, {3}}, 10, 10,
			want{used: []float64{0.5, 0.5}, ids: []string{"1", "2", "3"}, requests: 2}},
		{"emits the paid page before the budget", [][]int{{1, 2}, {3}}, 10, 0.5,
			want{used: []float64{0.5}, ids: []string{"1", "2"}, requests: 1, err: ErrBudget}},
		{"a page cut by the limit still reports", [][]int{{1, 2, 3}}, 2, 0.5,
			want{used: []float64{0.5}, ids: []string{"1", "2"}, requests: 1, err: ErrBudget}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, calls := dynamoServer(t, tc.pages)
			var used []float64
			total := 0.0
			q := Query{Text: "SELECT * FROM t", Limit: tc.limit, Use: func(amount float64) error {
				used = append(used, amount)
				if total += amount; total >= tc.max {
					return ErrBudget
				}
				return nil
			}}
			var ids []string
			err := d.Read(context.Background(), q, func(r record.Record) error {
				v, _ := r.Get("id")
				ids = append(ids, v.(string))
				return nil
			})
			for _, c := range *calls {
				assert.Equal(t, "TOTAL", c.ReturnConsumedCapacity)
			}
			assert.Equal(t, tc.want, want{used: used, ids: ids, requests: len(*calls), err: err})
		})
	}
}

func TestDynamoRecord(t *testing.T) {
	t.Parallel()
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberN{Value: "12345678901234567890"},
		"name": &types.AttributeValueMemberS{Value: "a"},
		"gone": &types.AttributeValueMemberNULL{Value: true},
		"tags": &types.AttributeValueMemberL{
			Value: []types.AttributeValue{&types.AttributeValueMemberBOOL{Value: true}},
		},
		"meta": &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{"k": &types.AttributeValueMemberSS{Value: []string{"x"}}},
		},
		"blob": &types.AttributeValueMemberB{Value: []byte{0xff}},
	}
	assert.Equal(
		t,
		`{"blob":"/w==","gone":null,"id":"12345678901234567890","meta":{"k":["x"]},"name":"a","tags":[true]}`,
		string(dynamoRecord(item).JSON()),
	)
}
