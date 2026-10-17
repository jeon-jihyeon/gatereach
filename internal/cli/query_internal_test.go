package cli

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/session"
)

// The report needs an End from a live session so it's checked from inside the package
func TestBudgetReport(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name   string
		client string
		max    float64
		end    session.End
		want   string
	}{
		{"under the budget", "athena", 10e9, session.End{Used: 1.23e9}, "db read 1.2 GB of --max 10 GB"},
		{"stopped at the budget", "athena", 1e9, session.End{Used: 1.5e9, Spent: true},
			"db stopped at --max: read 1.5 GB of 1 GB"},
		{"rows examined", "mysql", 5000, session.End{Used: 1200}, "db read 1200 rows examined of --max 5000 rows examined"},
		{"nothing read", "kafka", 10, session.End{}, "db read 0 records of --max 10 records"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, budgetReport("db", clients.Kinds[tc.client].Budget, tc.max, tc.end))
		})
	}
}

// These exit code rules need an interrupted state the public API can't produce
// so they're checked from inside the package
func TestExitFor(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("disk full")
	type args struct {
		follow      bool
		interrupted bool
		err         error
	}
	tcs := []struct {
		name string
		args args
		want error
	}{
		{"an interrupted single read exits 130", args{false, true, nil}, exitCode{code: exitInterrupted, msg: "interrupted"}},
		{"a sink close error after an interrupt exits 1", args{false, true, closeErr}, closeErr},
		{"an interrupted follow ends normally", args{true, true, nil}, nil},
		{"a failed run keeps its error", args{false, false, closeErr}, closeErr},
		{"a finished run exits 0", args{false, false, nil}, nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, exitFor(tc.args.follow, tc.args.interrupted, tc.args.err))
		})
	}
}

// A failed sink write needs a live session to reach the sink so it's checked from inside the package
func TestJoinErrors(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("disk full")
	runErr := errors.New("syntax error")
	tcs := []struct {
		name  string
		run   error
		close error
		want  string
	}{
		{"the same sink error is reported once", writeErr, writeErr, "disk full"},
		{"a wrapped sink error is reported once", fmt.Errorf("write: %w", writeErr), writeErr, "write: disk full"},
		{"a run error and a close error are both reported", runErr, writeErr, "syntax error\ndisk full"},
		{"only a close error", nil, writeErr, "disk full"},
		{"only a run error", runErr, nil, "syntax error"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, joinErrors(tc.run, tc.close), tc.want)
		})
	}
}

// Whether --timeout was given is visible only on the parsed options
func TestParseQuery_Timeout(t *testing.T) {
	t.Parallel()
	type want struct {
		timeout time.Duration
		set     bool
	}
	tcs := []struct {
		name string
		args []string
		want want
	}{
		{"left out takes the client's default later", []string{"r", "GET k"}, want{0, false}},
		{"an explicit 0 means none", []string{"r", "GET k", "--timeout", "0"}, want{0, true}},
		{"an explicit value", []string{"r", "GET k", "--timeout", "1.5"}, want{1500 * time.Millisecond, true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := &app{stdout: io.Discard, stderr: io.Discard}
			o, err := a.parseQuery(tc.args)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, want{o.timeout, o.timeoutSet})
		})
	}
}

func TestClean(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		in   string
		want string
	}{
		{"plain text stays", "unknown alias: café", "unknown alias: café"},
		{"newline and tab stay", "a\n\tb", "a\n\tb"},
		{"an escape sequence is shown", "a\x1b[2Jb", `a\u001B[2Jb`},
		{"carriage return and bell are shown", "a\r\a", `a\u000D\u0007`},
		{"DEL is shown", "a\x7f", `a\u007F`},
		{"a C1 control is shown", "a\u009bb", `a\u009Bb`},
		{"a lone C1 byte becomes the replacement character", "a\x9bb", "a\ufffdb"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clean(tc.in))
		})
	}
}
