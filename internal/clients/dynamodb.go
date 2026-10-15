package clients

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// 요청마다 평가할 최소 item 수
// Limit은 반환 건수가 아니라 평가 건수라 남은 건수만 넘기면 필터 쿼리가 item마다 요청
const pageMin = 1000

type dynamoClient struct {
	conn config.Conn
	db   *dynamodb.Client
}

// AWS 자격 증명은 프로필이 맡아 비밀값을 받지 않음
func newDynamoDB(conn config.Conn, _ string) Client {
	return &dynamoClient{conn: conn}
}

func dynamoCheck(text string, write bool) error {
	if !write && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(text)), "SELECT") {
		return errors.New("읽기 전용 모드에서는 SELECT만 허용\n쓰기가 필요하면 --write")
	}
	return nil
}

// 세션이 클라이언트 하나를 쥐어 자격 증명과 HTTP 연결을 재사용
func (d *dynamoClient) Connect(ctx context.Context) error {
	var opts []func(*awsconfig.LoadOptions) error
	if d.conn.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(d.conn.Profile))
	}
	if d.conn.Region != "" {
		opts = append(opts, awsconfig.WithRegion(d.conn.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return err
	}
	d.db = dynamodb.NewFromConfig(cfg)
	return nil
}

func (d *dynamoClient) Close() error { return nil }

// 페이지를 넘기며 item마다 바로 내보냄
func (d *dynamoClient) Read(ctx context.Context, q Query, emit Emit) error {
	if d.db == nil {
		return ErrDisconnected
	}
	left := q.Limit
	var token *string
	for left > 0 {
		out, err := d.db.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
			Statement: aws.String(q.Text),
			Limit:     aws.Int32(int32(min(max(left, pageMin), math.MaxInt32))),
			NextToken: token,
		})
		if err != nil {
			return err
		}
		for _, item := range out.Items {
			if left == 0 {
				return nil
			}
			if err := emit(dynamoRecord(item)); err != nil {
				return err
			}
			left--
		}
		if out.NextToken == nil {
			return nil
		}
		token = out.NextToken
	}
	return nil
}

func dynamoRecord(item map[string]types.AttributeValue) record.Record {
	names := make([]string, 0, len(item))
	for k := range item {
		names = append(names, k)
	}
	sort.Strings(names)
	values := make([]any, len(names))
	for i, k := range names {
		values[i] = attrValue(item[k])
	}
	return record.New(names, values)
}

// DynamoDB 타입 표기를 JSON 값으로
// N은 정밀도 보존을 위해 문자열 유지
// 바이너리는 base64 문자열
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
		return strings2any(x.Value)
	case *types.AttributeValueMemberNS:
		return strings2any(x.Value)
	case *types.AttributeValueMemberBS:
		out := make([]any, len(x.Value))
		for i, b := range x.Value {
			out[i] = base64.StdEncoding.EncodeToString(b)
		}
		return out
	}
	return nil
}

func strings2any(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
