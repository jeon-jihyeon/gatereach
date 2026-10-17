package clients

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

func athenaMock(t *testing.T, conn config.Conn) (*athenaClient, *MockAthenaAPI) {
	t.Helper()
	m := NewMockAthenaAPI(gomock.NewController(t))
	a := newAthena(conn, "").(*athenaClient)
	a.api = m
	a.pollMin, a.pollMax = time.Millisecond, 2*time.Millisecond
	return a, m
}

func athenaStarts(m *MockAthenaAPI) {
	m.EXPECT().
		StartQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.StartQueryExecutionOutput{QueryExecutionId: aws.String("q1")}, nil)
}

func athenaState(state types.QueryExecutionState, kind types.StatementType) *athena.GetQueryExecutionOutput {
	return &athena.GetQueryExecutionOutput{QueryExecution: &types.QueryExecution{
		StatementType: kind,
		Status:        &types.QueryExecutionStatus{State: state},
	}}
}

func athenaRow(values ...*string) types.Row {
	data := make([]types.Datum, len(values))
	for i, v := range values {
		data[i] = types.Datum{VarCharValue: v}
	}
	return types.Row{Data: data}
}

func TestAthenaRead(t *testing.T) {
	t.Parallel()
	type want struct {
		values     []string
		maxResults []int32
	}
	tcs := []struct {
		name  string
		kind  types.StatementType
		pages [][]string
		limit int
		want  want
	}{
		{"skips the header and follows the next token", types.StatementTypeDml, [][]string{{"v", "a", "b"}, {"c"}}, 10,
			want{values: []string{"a", "b", "c"}, maxResults: []int32{11, 8}}},
		{"stops mid page at the limit", types.StatementTypeDml, [][]string{{"v", "a", "b", "c"}, {"d"}}, 2,
			want{values: []string{"a", "b"}, maxResults: []int32{3}}},
		{"asks for at most a full page", types.StatementTypeDml, [][]string{{"v", "a"}}, 5000,
			want{values: []string{"a"}, maxResults: []int32{1000}}},
		{"the largest limit still asks for a full page", types.StatementTypeDml, [][]string{{"v", "a"}}, math.MaxInt,
			want{values: []string{"a"}, maxResults: []int32{1000}}},
		{"a SHOW result has no header", types.StatementTypeUtility, [][]string{{"v", "a"}}, 10,
			want{values: []string{"v", "a"}, maxResults: []int32{11}}},
		{"a first row that differs from the names is data", types.StatementTypeDml, [][]string{{"a", "b"}}, 10,
			want{values: []string{"a", "b"}, maxResults: []int32{11}}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			athenaStarts(m)
			gomock.InOrder(
				m.EXPECT().
					GetQueryExecution(gomock.Any(), gomock.Any()).
					Return(athenaState(types.QueryExecutionStateRunning, tc.kind), nil),
				m.EXPECT().
					GetQueryExecution(gomock.Any(), gomock.Any()).
					Return(athenaState(types.QueryExecutionStateSucceeded, tc.kind), nil),
			)
			var maxResults []int32
			call := 0
			m.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).DoAndReturn(
				func(
					_ context.Context, in *athena.GetQueryResultsInput, _ ...func(*athena.Options),
				) (*athena.GetQueryResultsOutput, error) {
					assert.Equal(t, "q1", aws.ToString(in.QueryExecutionId))
					if call == 0 {
						assert.Nil(t, in.NextToken)
					} else {
						assert.Equal(t, "t1", aws.ToString(in.NextToken))
					}
					maxResults = append(maxResults, aws.ToInt32(in.MaxResults))
					rows := make([]types.Row, len(tc.pages[call]))
					for i, v := range tc.pages[call] {
						rows[i] = athenaRow(aws.String(v))
					}
					out := &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{
						Rows: rows,
						ResultSetMetadata: &types.ResultSetMetadata{
							ColumnInfo: []types.ColumnInfo{{Name: aws.String("v"), Type: aws.String("varchar")}},
						},
					}}
					call++
					if call < len(tc.pages) {
						out.NextToken = aws.String("t1")
					}
					return out, nil
				}).
				MinTimes(1)
			var values []string
			err := a.Read(
				context.Background(),
				Query{Text: "SELECT v FROM t", Limit: tc.limit},
				func(r record.Record) error {
					v, _ := r.Get("v")
					values = append(values, v.(string))
					return nil
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want.values, values)
			assert.Equal(t, tc.want.maxResults, maxResults)
		})
	}
}

func TestAthenaRecord(t *testing.T) {
	t.Parallel()
	cols := []types.ColumnInfo{
		{Name: aws.String("tiny"), Type: aws.String("tinyint")},
		{Name: aws.String("small"), Type: aws.String("smallint")},
		{Name: aws.String("n"), Type: aws.String("integer")},
		{Name: aws.String("big"), Type: aws.String("bigint")},
		{Name: aws.String("ok"), Type: aws.String("boolean")},
		{Name: aws.String("ratio"), Type: aws.String("double")},
		{Name: aws.String("price"), Type: aws.String("decimal")},
		{Name: aws.String("day"), Type: aws.String("date")},
		{Name: aws.String("tags"), Type: aws.String("array")},
		{Name: aws.String("empty"), Type: aws.String("varchar")},
		{Name: aws.String("gone"), Type: aws.String("bigint")},
		{Name: aws.String("gone"), Type: aws.String("varchar")},
	}
	row := athenaRow(
		aws.String("-1"),
		aws.String("7"),
		aws.String("42"),
		aws.String("9007199254740993"),
		aws.String("true"),
		aws.String(
			"0.5",
		),
		aws.String("12.30"),
		aws.String("2026-01-02"),
		aws.String("[a, b]"),
		aws.String(""),
		nil,
		nil,
	)
	got := athenaRecord(athenaNames(cols), cols, row)
	assert.Equal(t, `{"tiny":-1,"small":7,"n":42,"big":9007199254740993,"ok":true,"ratio":"0.5","price":"12.30",`+
		`"day":"2026-01-02","tags":"[a, b]","empty":"","gone":null,"gone_2":null}`, string(got.JSON()))
}

func TestAthenaFailure(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name   string
		status types.QueryExecutionStatus
		err    string
	}{
		{"failed with the state change reason", types.QueryExecutionStatus{State: types.QueryExecutionStateFailed,
			StateChangeReason: aws.String("TABLE_NOT_FOUND: line 1:15: Table 'acme.t' does not exist")},
			"athena query failed: TABLE_NOT_FOUND: line 1:15: Table 'acme.t' does not exist"},
		{"failed with only the Athena error", types.QueryExecutionStatus{State: types.QueryExecutionStateFailed,
			AthenaError: &types.AthenaError{
				ErrorMessage: aws.String("access denied"),
			}}, "athena query failed: access denied"},
		{"cancelled elsewhere", types.QueryExecutionStatus{State: types.QueryExecutionStateCancelled},
			"athena query cancelled"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			athenaStarts(m)
			status := tc.status
			m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).Return(
				&athena.GetQueryExecutionOutput{QueryExecution: &types.QueryExecution{Status: &status}}, nil)
			err := a.Read(
				context.Background(),
				Query{Text: "SELECT 1", Limit: 10},
				func(record.Record) error { return nil },
			)
			assert.EqualError(t, err, tc.err)
		})
	}
}

func TestAthenaStopsAnUnfinishedQuery(t *testing.T) {
	t.Parallel()
	statusErr := errors.New("throttled")
	tcs := []struct {
		name string
		// Returns the request context and makes GetQueryExecution answer as the case needs
		setup func(t *testing.T, m *MockAthenaAPI) context.Context
		err   string
	}{
		{"a cancelled request", func(t *testing.T, m *MockAthenaAPI) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(
					context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options),
				) (*athena.GetQueryExecutionOutput, error) {
					cancel()
					return athenaState(types.QueryExecutionStateRunning, types.StatementTypeDml), nil
				})
			return ctx
		}, context.Canceled.Error()},
		{"a poll that runs past the deadline", func(t *testing.T, m *MockAthenaAPI) context.Context {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			t.Cleanup(cancel)
			m.EXPECT().
				GetQueryExecution(gomock.Any(), gomock.Any()).
				Return(athenaState(types.QueryExecutionStateQueued, types.StatementTypeDml), nil).
				MinTimes(1)
			return ctx
		}, context.DeadlineExceeded.Error()},
		{"a status call that fails", func(t *testing.T, m *MockAthenaAPI) context.Context {
			m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).Return(nil, statusErr)
			return context.Background()
		}, statusErr.Error()},
		{"a reply without a status", func(t *testing.T, m *MockAthenaAPI) context.Context {
			m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).Return(&athena.GetQueryExecutionOutput{}, nil)
			return context.Background()
		}, "athena query q1 came back without a status"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			athenaStarts(m)
			ctx := tc.setup(t, m)
			m.EXPECT().StopQueryExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(
					ctx context.Context, in *athena.StopQueryExecutionInput, _ ...func(*athena.Options),
				) (*athena.StopQueryExecutionOutput, error) {
					assert.NoError(t, ctx.Err(), "the stop call must outlive the request")
					assert.Equal(t, "q1", aws.ToString(in.QueryExecutionId))
					return &athena.StopQueryExecutionOutput{}, nil
				})
			// The status after the stop
			// A poll that keeps answering takes it in the deadline case
			m.EXPECT().
				GetQueryExecution(gomock.Any(), gomock.Any()).
				Return(&athena.GetQueryExecutionOutput{}, nil).
				MaxTimes(1)
			err := a.Read(ctx, Query{Text: "SELECT 1", Limit: 10}, func(record.Record) error { return nil })
			assert.EqualError(t, err, tc.err)
		})
	}
}

// What a stopped query scanned until the stop counts toward --max
func TestAthenaStoppedUse(t *testing.T) {
	t.Parallel()
	statusErr := errors.New("throttled")
	bare := athenaState(types.QueryExecutionStateCancelled, types.StatementTypeDml)
	scanned := athenaState(types.QueryExecutionStateCancelled, types.StatementTypeDml)
	scanned.QueryExecution.Statistics = &types.QueryExecutionStatistics{DataScannedInBytes: aws.Int64(900)}
	tcs := []struct {
		name string
		// The reply to the status call after the stop
		out  *athena.GetQueryExecutionOutput
		err  error
		used []float64
	}{
		{"the scan so far is reported", scanned, nil, []float64{900}},
		{"a status call that fails reports nothing", nil, statusErr, nil},
		{"no statistics reports nothing", bare, nil, nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			athenaStarts(m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gomock.InOrder(
				m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(
						context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options),
					) (*athena.GetQueryExecutionOutput, error) {
						cancel()
						return athenaState(types.QueryExecutionStateRunning, types.StatementTypeDml), nil
					}),
				m.EXPECT().StopQueryExecution(gomock.Any(), gomock.Any()).Return(&athena.StopQueryExecutionOutput{}, nil),
				m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(
						ctx context.Context, _ *athena.GetQueryExecutionInput, _ ...func(*athena.Options),
					) (*athena.GetQueryExecutionOutput, error) {
						assert.NoError(t, ctx.Err(), "the status call after the stop must outlive the request")
						return tc.out, tc.err
					}),
			)
			var used []float64
			q := Query{Text: "SELECT 1", Limit: 10, Use: func(amount float64) error {
				used = append(used, amount)
				return nil
			}}
			err := a.Read(ctx, q, func(record.Record) error { return nil })
			assert.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, tc.used, used)
		})
	}
}

func TestAthenaReadEnds(t *testing.T) {
	t.Parallel()
	startErr := errors.New("start refused")
	resultsErr := errors.New("results refused")
	emitErr := errors.New("sink closed")
	succeeds := func(m *MockAthenaAPI) {
		athenaStarts(m)
		m.EXPECT().
			GetQueryExecution(gomock.Any(), gomock.Any()).
			Return(athenaState(types.QueryExecutionStateSucceeded, types.StatementTypeDml), nil)
	}
	page := &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{
		Rows: []types.Row{
			athenaRow(aws.String("v")),
			athenaRow(aws.String("a")),
			athenaRow(aws.String("b")),
		},
		ResultSetMetadata: &types.ResultSetMetadata{
			ColumnInfo: []types.ColumnInfo{{Name: aws.String("v"), Type: aws.String("varchar")}},
		},
	}}
	type want struct {
		err     error
		emitted int
	}
	tcs := []struct {
		name    string
		init    func(m *MockAthenaAPI)
		emitErr error
		want    want
	}{
		{"a start that fails", func(m *MockAthenaAPI) {
			m.EXPECT().StartQueryExecution(gomock.Any(), gomock.Any()).Return(nil, startErr)
		}, nil, want{err: startErr}},
		{"a results call that fails", func(m *MockAthenaAPI) {
			succeeds(m)
			m.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).Return(nil, resultsErr)
		}, nil, want{err: resultsErr}},
		{"a reply without a result set ends empty", func(m *MockAthenaAPI) {
			succeeds(m)
			m.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).Return(&athena.GetQueryResultsOutput{}, nil)
		}, nil, want{}},
		{"an emit that fails stops the stream", func(m *MockAthenaAPI) {
			succeeds(m)
			m.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).Return(page, nil)
		}, emitErr, want{err: emitErr, emitted: 1}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			tc.init(m)
			emitted := 0
			err := a.Read(context.Background(), Query{Text: "SELECT v FROM t", Limit: 10}, func(record.Record) error {
				emitted++
				return tc.emitErr
			})
			assert.ErrorIs(t, err, tc.want.err)
			assert.Equal(t, tc.want.emitted, emitted)
		})
	}
}

func TestAthenaUse(t *testing.T) {
	t.Parallel()
	page := &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{
		Rows: []types.Row{
			athenaRow(aws.String("v")),
			athenaRow(aws.String("a")),
			athenaRow(aws.String("b")),
		},
		ResultSetMetadata: &types.ResultSetMetadata{
			ColumnInfo: []types.ColumnInfo{{Name: aws.String("v"), Type: aws.String("varchar")}},
		},
	}}
	type want struct {
		used    []float64
		emitted int
		err     string
	}
	tcs := []struct {
		name  string
		state types.QueryExecutionState
		// nil leaves Statistics out
		scanned *int64
		useErr  error
		want    want
	}{
		{"a finished query reports its scan", types.QueryExecutionStateSucceeded, aws.Int64(1500), nil,
			want{used: []float64{1500}, emitted: 2}},
		{"the paid rows come out before the budget", types.QueryExecutionStateSucceeded, aws.Int64(1500), ErrBudget,
			want{used: []float64{1500}, emitted: 2, err: ErrBudget.Error()}},
		{"a failed query reports its scan", types.QueryExecutionStateFailed, aws.Int64(700), nil,
			want{used: []float64{700}, err: "athena query failed"}},
		{"a cancelled query reports its scan", types.QueryExecutionStateCancelled, aws.Int64(300), nil,
			want{used: []float64{300}, err: "athena query cancelled"}},
		{"a failure wins over the budget", types.QueryExecutionStateFailed, aws.Int64(700), ErrBudget,
			want{used: []float64{700}, err: "athena query failed"}},
		{"no statistics reports nothing scanned", types.QueryExecutionStateSucceeded, nil, nil,
			want{used: []float64{0}, emitted: 2}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, m := athenaMock(t, config.Conn{Client: "athena", Workgroup: "primary"})
			athenaStarts(m)
			out := athenaState(tc.state, types.StatementTypeDml)
			if tc.scanned != nil {
				out.QueryExecution.Statistics = &types.QueryExecutionStatistics{DataScannedInBytes: tc.scanned}
			}
			m.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).Return(out, nil)
			m.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).Return(page, nil).MaxTimes(1)
			var used []float64
			emitted := 0
			q := Query{Text: "SELECT v FROM t", Limit: 10, Use: func(amount float64) error {
				used = append(used, amount)
				return tc.useErr
			}}
			err := a.Read(context.Background(), q, func(record.Record) error {
				emitted++
				return nil
			})
			assert.Equal(t, tc.want, want{used: used, emitted: emitted, err: errText(err)})
		})
	}
}

func TestAthenaStartInput(t *testing.T) {
	t.Parallel()
	type want struct {
		catalog, database, output string
	}
	tcs := []struct {
		name string
		conn config.Conn
		want want
	}{
		{"defaults to AwsDataCatalog and the workgroup's output", config.Conn{Workgroup: "primary"},
			want{catalog: "AwsDataCatalog"}},
		{
			"passes the catalog, database and output location",
			config.Conn{
				Workgroup:      "primary",
				Catalog:        "lake",
				Database:       "acme",
				OutputLocation: "s3://example-bucket/r/",
			},
			want{catalog: "lake", database: "acme", output: "s3://example-bucket/r/"},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := newAthena(tc.conn, "").(*athenaClient).startInput("SELECT 1")
			assert.Equal(t, "SELECT 1", aws.ToString(in.QueryString))
			assert.Equal(t, "primary", aws.ToString(in.WorkGroup))
			assert.Equal(t, tc.want.catalog, aws.ToString(in.QueryExecutionContext.Catalog))
			assert.Equal(t, tc.want.database, aws.ToString(in.QueryExecutionContext.Database))
			var output string
			if in.ResultConfiguration != nil {
				output = aws.ToString(in.ResultConfiguration.OutputLocation)
			}
			assert.Equal(t, tc.want.output, output)
		})
	}
}

func TestAthenaCheckPassthrough(t *testing.T) {
	t.Parallel()
	assert.Equal(t,
		"read-only mode refuses system.query: it sends native SQL to the federated source\nuse --write to run it anyway",
		errText(athenaCheck("SELECT * FROM TABLE(system.query(query => 'DELETE FROM t'))", false)))
}
