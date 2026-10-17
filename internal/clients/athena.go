package clients

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=athena_mock_test.go -package=clients -self_package=github.com/jeon-jihyeon/gatereach/internal/clients -mock_names=athenaAPI=MockAthenaAPI github.com/jeon-jihyeon/gatereach/internal/clients athenaAPI

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const (
	athenaCatalog = "AwsDataCatalog"
	// The most rows GetQueryResults returns per page
	athenaPage     = 1000
	athenaPollMin  = 100 * time.Millisecond
	athenaPollMax  = 2 * time.Second
	athenaStopWait = 5 * time.Second
)

// Only the calls the driver makes so tests can swap in a mock
type athenaAPI interface {
	StartQueryExecution(ctx context.Context, in *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, in *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, in *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
	StopQueryExecution(ctx context.Context, in *athena.StopQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
}

type athenaClient struct {
	conn    config.Conn
	api     athenaAPI
	pollMin time.Duration
	pollMax time.Duration
}

func newAthena(conn config.Conn, _ string) Client {
	return &athenaClient{conn: conn, pollMin: athenaPollMin, pollMax: athenaPollMax}
}

func athenaValidate(conn config.Conn) error {
	if conn.OutputLocation != "" && !strings.HasPrefix(conn.OutputLocation, "s3://") {
		return fmt.Errorf("athena output_location must start with s3://: %s", conn.OutputLocation)
	}
	return nil
}

// 1. Athena runs one statement per call so only the leading keyword decides
// 2. EXPLAIN ANALYZE runs its statement so the statement after it must be a read too
func athenaCheck(text string, write bool) error {
	if write {
		return nil
	}
	if athenaPassthrough(text) {
		return errors.New("read-only mode refuses system.query: it sends native SQL to the federated source\n" +
			"use --write to run it anyway")
	}
	words := leadingWords(text, 5)
	switch wordAt(words, 0) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE":
		return nil
	case "EXPLAIN":
		if wordAt(words, 1) != "ANALYZE" {
			return nil
		}
		// The statement follows an optional FORMAT option or VERBOSE
		i := 2
		switch wordAt(words, i) {
		case "FORMAT":
			i += 2
		case "VERBOSE":
			i++
		}
		if next := wordAt(words, i); next == "SELECT" || next == "WITH" {
			return nil
		}
		return errors.New("read-only mode allows EXPLAIN ANALYZE only on SELECT or WITH\nuse --write to run it anyway")
	}
	return errors.New("read-only mode allows only SELECT, WITH, SHOW, DESCRIBE and EXPLAIN\nuse --write to write")
}

// Whether the text calls the passthrough function `system.query` outside strings and comments
// 1. A federated catalog runs its argument on the remote source unchecked
// 2. Names in double quotes count since the engine reads `"system"."query"` the same way
func athenaPassthrough(text string) bool {
	var names []string
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == '\'' || c == '"':
			// A doubled quote stays inside
			j := i + 1
			for j < len(text) && (text[j] != c || j+1 < len(text) && text[j+1] == c) {
				if text[j] == c {
					j++
				}
				j++
			}
			if c == '"' {
				names = append(names, strings.ToLower(strings.ReplaceAll(text[i+1:min(j, len(text))], `""`, `"`)))
			} else {
				names = append(names, "'")
			}
			i = j + 1
		case strings.HasPrefix(text[i:], "--"):
			for i < len(text) && text[i] != '\r' && text[i] != '\n' {
				i++
			}
		case strings.HasPrefix(text[i:], "/*"):
			end := strings.Index(text[i+2:], "*/")
			if end < 0 {
				end = len(text) - i - 2
			}
			i += 2 + end + 2
		case unicode.IsSpace(rune(c)):
			i++
		case c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i
			for j < len(text) && (text[j] == '_' || text[j] >= '0' && text[j] <= '9' ||
				text[j] >= 'a' && text[j] <= 'z' || text[j] >= 'A' && text[j] <= 'Z') {
				j++
			}
			names = append(names, strings.ToLower(text[i:j]))
			i = j
		default:
			names = append(names, string(c))
			i++
		}
	}
	for i := 0; i+2 < len(names); i++ {
		if names[i] == "system" && names[i+1] == "." && names[i+2] == "query" {
			return true
		}
	}
	return false
}

func wordAt(words []string, i int) string {
	if i < len(words) {
		return words[i]
	}
	return ""
}

// Up to n upper-cased keywords from the start
// 1. Spaces and SQL comments may come before any keyword
// 2. Parentheses are skipped too since a query may open with one and an option list closes before its statement
func leadingWords(text string, n int) []string {
	var words []string
	for len(words) < n {
		text = strings.TrimLeftFunc(text, func(r rune) bool { return unicode.IsSpace(r) || r == '(' || r == ')' })
		switch {
		case strings.HasPrefix(text, "--"):
			// The engine ends a line comment at CR as well as LF
			end := strings.IndexAny(text, "\r\n")
			if end < 0 {
				return words
			}
			text = text[end+1:]
		case strings.HasPrefix(text, "/*"):
			end := strings.Index(text[2:], "*/")
			if end < 0 {
				return words
			}
			text = text[2+end+2:]
		default:
			end := strings.IndexFunc(text, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' })
			if end < 0 {
				end = len(text)
			}
			if end == 0 {
				return words
			}
			words = append(words, strings.ToUpper(text[:end]))
			text = text[end:]
		}
	}
	return words
}

// The SDK client holds credentials and HTTP connections across requests
func (a *athenaClient) Connect(ctx context.Context) error {
	cfg, err := awsConfig(ctx, a.conn.Profile, a.conn.Region)
	if err != nil {
		return err
	}
	a.api = athena.NewFromConfig(cfg)
	return nil
}

func (a *athenaClient) Close() error { return nil }

func (a *athenaClient) Read(ctx context.Context, q Query, emit Emit) error {
	if a.api == nil {
		return ErrDisconnected
	}
	start, err := a.api.StartQueryExecution(ctx, a.startInput(q.Text))
	if err != nil {
		return err
	}
	exec, err := a.wait(ctx, start.QueryExecutionId)
	if exec == nil {
		return err
	}
	// A failed or stopped query is billed for what it scanned too
	used := q.use(athenaScanned(exec))
	if err != nil {
		return err
	}
	if err := a.results(ctx, start.QueryExecutionId, exec.StatementType, q.Limit, emit); err != nil {
		return err
	}
	return used
}

func athenaScanned(exec *types.QueryExecution) float64 {
	if exec.Statistics == nil {
		return 0
	}
	return float64(aws.ToInt64(exec.Statistics.DataScannedInBytes))
}

func (a *athenaClient) startInput(text string) *athena.StartQueryExecutionInput {
	catalog := a.conn.Catalog
	if catalog == "" {
		catalog = athenaCatalog
	}
	in := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(text),
		WorkGroup:             aws.String(a.conn.Workgroup),
		QueryExecutionContext: &types.QueryExecutionContext{Catalog: aws.String(catalog)},
	}
	if a.conn.Database != "" {
		in.QueryExecutionContext.Database = aws.String(a.conn.Database)
	}
	if a.conn.OutputLocation != "" {
		in.ResultConfiguration = &types.ResultConfiguration{OutputLocation: aws.String(a.conn.OutputLocation)}
	}
	return in
}

// Polls with a doubling wait until the query ends
// 1. Leaving before then stops the query since Athena would keep scanning and billing otherwise
// 2. A failed or cancelled query comes back with its error so its statistics still count
// 3. A stopped query comes back with its error too when Athena reports what it scanned
func (a *athenaClient) wait(ctx context.Context, id *string) (*types.QueryExecution, error) {
	delay := a.pollMin
	for {
		out, err := a.api.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: id})
		if err != nil {
			return a.stop(ctx, id), err
		}
		exec := out.QueryExecution
		if exec == nil || exec.Status == nil {
			return a.stop(ctx, id), fmt.Errorf("athena query %s came back without a status", aws.ToString(id))
		}
		switch exec.Status.State {
		case types.QueryExecutionStateSucceeded:
			return exec, nil
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			return exec, athenaFailure(exec.Status)
		}
		select {
		case <-ctx.Done():
			return a.stop(ctx, id), ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, a.pollMax)
	}
}

// Runs even after ctx ends
// A cancelled request is exactly when the query must stop
// Returns the stopped query when Athena reports what it scanned so far and nil otherwise
func (a *athenaClient) stop(ctx context.Context, id *string) *types.QueryExecution {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), athenaStopWait)
	defer cancel()
	_, _ = a.api.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{QueryExecutionId: id})
	out, err := a.api.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: id})
	if err != nil || out.QueryExecution == nil || out.QueryExecution.Statistics == nil ||
		out.QueryExecution.Statistics.DataScannedInBytes == nil {
		return nil
	}
	return out.QueryExecution
}

func athenaFailure(s *types.QueryExecutionStatus) error {
	state := strings.ToLower(string(s.State))
	reason := aws.ToString(s.StateChangeReason)
	if reason == "" && s.AthenaError != nil {
		reason = aws.ToString(s.AthenaError.ErrorMessage)
	}
	if reason == "" {
		return fmt.Errorf("athena query %s", state)
	}
	return fmt.Errorf("athena query %s: %s", state, reason)
}

// Streams page by page and stops mid page at the limit
func (a *athenaClient) results(ctx context.Context, id *string, kind types.StatementType, limit int, emit Emit) error {
	left := limit
	var token *string
	var names []string
	var cols []types.ColumnInfo
	for left > 0 {
		first := token == nil
		size := min(left, athenaPage)
		// The first page may open with the header row
		if first && size < athenaPage {
			size++
		}
		out, err := a.api.GetQueryResults(ctx, &athena.GetQueryResultsInput{
			QueryExecutionId: id,
			NextToken:        token,
			MaxResults:       aws.Int32(int32(size)),
		})
		if err != nil {
			return err
		}
		set := out.ResultSet
		if set == nil {
			return nil
		}
		rows := set.Rows
		if first {
			if set.ResultSetMetadata != nil {
				cols = set.ResultSetMetadata.ColumnInfo
			}
			names = athenaNames(cols)
			if athenaHeader(kind, cols, rows) {
				rows = rows[1:]
			}
		}
		for _, row := range rows {
			if left == 0 {
				return nil
			}
			if err := emit(athenaRecord(names, cols, row)); err != nil {
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

func athenaNames(cols []types.ColumnInfo) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = aws.ToString(c.Name)
	}
	return record.UniqueNames(names)
}

// A DML result opens with the column names as its first row
// SHOW and DDL results don't
// Matching the names too keeps a real first row when Athena leaves the header out
func athenaHeader(kind types.StatementType, cols []types.ColumnInfo, rows []types.Row) bool {
	if kind != types.StatementTypeDml || len(rows) == 0 || len(rows[0].Data) != len(cols) {
		return false
	}
	for i, d := range rows[0].Data {
		if aws.ToString(d.VarCharValue) != aws.ToString(cols[i].Name) {
			return false
		}
	}
	return true
}

func athenaRecord(names []string, cols []types.ColumnInfo, row types.Row) record.Record {
	values := make([]any, len(names))
	for i := range names {
		if i < len(row.Data) {
			values[i] = athenaValue(aws.ToString(cols[i].Type), row.Data[i].VarCharValue)
		}
	}
	return record.New(names, values)
}

// Athena sends every value as text and a NULL as no value at all
// 1. Integers and booleans get their JSON type
// 2. Decimals, floats and dates stay strings so no precision or format is lost
func athenaValue(typ string, v *string) any {
	if v == nil {
		return nil
	}
	switch strings.ToLower(typ) {
	case "tinyint", "smallint", "integer", "int", "bigint":
		if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
			return n
		}
	case "boolean":
		if b, err := strconv.ParseBool(*v); err == nil {
			return b
		}
	}
	return *v
}
