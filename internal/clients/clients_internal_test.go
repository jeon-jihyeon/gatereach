package clients

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The steps share one seen so each runs after the one before
func TestSeenForgetsTheOldest(t *testing.T) {
	t.Parallel()
	s := newSeen(1)
	tcs := []struct {
		name string
		id   string
		want bool
	}{
		{"first is new", "a", true},
		{"repeat is not new", "a", false},
		{"second evicts the first", "b", true},
		{"the first is new again", "a", true},
	}
	for _, tc := range tcs {
		assert.Equal(t, tc.want, s.add([]byte(tc.id)), tc.name)
	}
}

// A context whose deadline has passed while its timer hasn't fired yet
type unfiredContext struct {
	context.Context
	deadline time.Time
}

func (c unfiredContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestRunPastDeadline(t *testing.T) {
	t.Parallel()
	past := time.Now().Add(-time.Millisecond)
	socket := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	tcs := []struct {
		name string
		ctx  context.Context
		run  context.Context
		err  error
		want bool
	}{
		{"a socket deadline before the run's timer fires", context.Background(),
			unfiredContext{context.Background(), past}, socket, true},
		{"a socket deadline before the run's deadline is the driver's own", context.Background(),
			unfiredContext{context.Background(), time.Now().Add(time.Hour)}, socket, false},
		{"a request deadline that passed keeps its error", unfiredContext{context.Background(), past},
			unfiredContext{context.Background(), past}, socket, false},
		{"another error past the deadline keeps its message", context.Background(),
			unfiredContext{context.Background(), past}, errors.New("dropped"), false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, runPastDeadline(tc.ctx, tc.run, tc.err))
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
