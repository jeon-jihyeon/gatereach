package clients

import (
	"context"
	"encoding/json"
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

// ExecuteStatement 페이지를 차례로 돌려주는 HTTP 서버와 받은 Limit 기록
func dynamoServer(t *testing.T, pages [][]int) (*dynamoClient, *[]int) {
	t.Helper()
	var mu sync.Mutex
	var limits []int
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in struct{ Limit int }
		_ = json.Unmarshal(body, &in)
		mu.Lock()
		limits = append(limits, in.Limit)
		page := pages[call]
		call++
		last := call == len(pages)
		mu.Unlock()
		items := make([]map[string]any, len(page))
		for i, id := range page {
			items[i] = map[string]any{"id": map[string]string{"N": fmt.Sprint(id)}}
		}
		out := map[string]any{"Items": items}
		if !last {
			out["NextToken"] = fmt.Sprintf("t%d", call)
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	d := newDynamoDB(config.Conn{Client: "dynamodb"}, "").(*dynamoClient)
	d.db = dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
	})
	return d, &limits
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
		{"asks for the limit when it is above the page minimum", [][]int{{1}}, 5000, want{ids: []string{"1"}, limits: []int{5000}}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, limits := dynamoServer(t, tc.pages)
			var ids []string
			err := d.Read(context.Background(), Query{Text: "SELECT * FROM t", Limit: tc.limit}, func(r record.Record) error {
				v, _ := r.Get("id")
				ids = append(ids, v.(string))
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want.ids, ids)
			assert.Equal(t, tc.want.limits, *limits)
		})
	}
}

func TestDynamoRecord(t *testing.T) {
	t.Parallel()
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberN{Value: "12345678901234567890"},
		"name": &types.AttributeValueMemberS{Value: "a"},
		"gone": &types.AttributeValueMemberNULL{Value: true},
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberBOOL{Value: true}}},
		"meta": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": &types.AttributeValueMemberSS{Value: []string{"x"}}}},
		"blob": &types.AttributeValueMemberB{Value: []byte{0xff}},
	}
	assert.Equal(t, `{"blob":"/w==","gone":null,"id":"12345678901234567890","meta":{"k":["x"]},"name":"a","tags":[true]}`, string(dynamoRecord(item).JSON()))
}
