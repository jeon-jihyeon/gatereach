package clients

import (
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Minimum items evaluated per request
// Limit counts evaluated items instead of returned ones
// Passing only the remaining count would send a filtered query one request per item
const dynamoPageMin = 1000

type dynamoClient struct {
	conn config.Conn
	db   *dynamodb.Client
}

// The AWS profile supplies credentials so no secret is taken
func newDynamoDB(conn config.Conn, _ string) Client {
	return &dynamoClient{conn: conn}
}

func dynamoCheck(text string, write bool) error {
	if !write && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(text)), "SELECT") {
		return errors.New("read-only mode allows only SELECT\nuse --write to write")
	}
	return nil
}

// The session holds one client to reuse credentials and HTTP connections
func (d *dynamoClient) Connect(ctx context.Context) error {
	cfg, err := awsConfig(ctx, d.conn.Profile, d.conn.Region)
	if err != nil {
		return err
	}
	d.db = dynamodb.NewFromConfig(cfg)
	return nil
}

func (d *dynamoClient) Close() error { return nil }

// Emits each item as its page arrives
func (d *dynamoClient) Read(ctx context.Context, q Query, emit Emit) error {
	if d.db == nil {
		return ErrDisconnected
	}
	// Capacity is asked for only when it's measured
	var capacity types.ReturnConsumedCapacity
	if q.Use != nil {
		capacity = types.ReturnConsumedCapacityTotal
	}
	left := q.Limit
	var token *string
	for left > 0 {
		out, err := d.db.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
			Statement:              aws.String(q.Text),
			Limit:                  aws.Int32(int32(min(max(left, dynamoPageMin), math.MaxInt32))),
			NextToken:              token,
			ReturnConsumedCapacity: capacity,
		})
		if err != nil {
			return err
		}
		used := q.use(dynamoCapacity(out.ConsumedCapacity))
		for _, item := range out.Items {
			if left == 0 {
				break
			}
			if err := emit(dynamoRecord(item)); err != nil {
				return err
			}
			left--
		}
		if used != nil || out.NextToken == nil {
			return used
		}
		token = out.NextToken
	}
	return nil
}

func dynamoCapacity(c *types.ConsumedCapacity) float64 {
	if c == nil {
		return 0
	}
	return aws.ToFloat64(c.CapacityUnits)
}

func dynamoRecord(item map[string]types.AttributeValue) record.Record {
	names := slices.Sorted(maps.Keys(item))
	values := make([]any, len(names))
	for i, k := range names {
		values[i] = attrValue(item[k])
	}
	return record.New(names, values)
}

// 1. N and NS stay strings to keep their precision
// 2. B and BS become base64 strings
func attrValue(v types.AttributeValue) any {
	switch x := v.(type) {
	case *types.AttributeValueMemberS:
		return x.Value
	case *types.AttributeValueMemberN:
		return x.Value
	case *types.AttributeValueMemberBOOL:
		return x.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberB:
		return base64.StdEncoding.EncodeToString(x.Value)
	case *types.AttributeValueMemberM:
		out := make(map[string]any, len(x.Value))
		for k, e := range x.Value {
			out[k] = attrValue(e)
		}
		return out
	case *types.AttributeValueMemberL:
		out := make([]any, len(x.Value))
		for i, e := range x.Value {
			out[i] = attrValue(e)
		}
		return out
	case *types.AttributeValueMemberSS:
		return anySlice(x.Value)
	case *types.AttributeValueMemberNS:
		return anySlice(x.Value)
	case *types.AttributeValueMemberBS:
		out := make([]any, len(x.Value))
		for i, b := range x.Value {
			out[i] = base64.StdEncoding.EncodeToString(b)
		}
		return out
	}
	return nil
}

func anySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
