// Package clients turns each source driver into one Record stream
package clients

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=mocks/client.go -package=mocks github.com/jeon-jihyeon/gatereach/internal/clients Client,Follower

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Returning an error stops the stream
type Emit func(record.Record) error

type Query struct {
	Text  string
	Write bool
	Limit int
	// Run ends one run past it
	// 0 means no limit
	Timeout time.Duration
	// Takes what the source read for the work just done in the unit of its Budget
	// 1. A client calls it after the work and emits that work's results before returning its error
	// 2. nil means nothing is measured
	Use func(amount float64) error
}

// Passes the amount on when --max is set
func (q Query) use(amount float64) error {
	if q.Use == nil {
		return nil
	}
	return q.Use(amount)
}

// The connection dropped and nothing ran
// The session reconnects and retries once
var ErrDisconnected = errors.New("disconnected")

// What the source read reached --max
// The records the paid work returned are emitted before it
var ErrBudget = errors.New("budget reached")

// The session holds the connection Connect opened until Close and reuses it for every request
type Client interface {
	// Cleans up whatever it opened when it fails
	Connect(ctx context.Context) error
	// Emits at most Limit records
	// Returns ErrDisconnected when a dropped connection ran nothing
	Read(ctx context.Context, q Query, emit Emit) error
	Close() error
}

// A client whose source pushes new records instead of being polled
type Follower interface {
	// ok is false for a query it doesn't handle
	Follow(ctx context.Context, q Query, emit Emit) (ok bool, err error)
}

type Kind struct {
	// Connects through host and port so the tunnel has to open first
	Networked bool
	Required  []string
	// Write rules don't depend on the connection
	// so they run before anything connects
	Check func(text string, write bool) error
	// Checks setting combinations beyond the required keys
	// nil skips the check
	Validate func(conn config.Conn) error
	// password is the secret the session resolved once
	New    func(conn config.Conn, password string) Client
	Budget Budget
	// The per-run limit the CLI applies when --timeout isn't given
	// 0 leaves a run unlimited
	Timeout time.Duration
}

// The unit --max counts in
type Budget struct {
	// Named in the message for a bad --max
	Unit string
	// A valid --max shown next to the unit
	Example string
	// Turns --max into an amount before any connection
	Parse func(text string) (float64, error)
	// Shows an amount in the report
	Format func(amount float64) string
}

// Default timeouts
// 1. MySQL and Redis are shared servers so a forgotten query must not hold them for long
// 2. DynamoDB pages through a table on its own capacity so it gets longer
// 3. Athena runs long by design and --max bounds its cost
// 4. Kafka follows a topic without end
var Kinds = map[string]Kind{
	"mysql": {
		Networked: true, Required: []string{"port", "user"}, Check: mysqlCheck, New: newMySQL, Timeout: 30 * time.Second,
		Budget: Budget{Unit: "rows examined", Example: "100000", Parse: parseWhole,
			Format: formatCount("row examined", "rows examined")},
	},
	"redis": {
		Networked: true, Required: []string{"port"}, Check: redisCheck, New: newRedis, Timeout: 10 * time.Second,
		Budget: Budget{Unit: "reply elements", Example: "1000", Parse: parseWhole,
			Format: formatCount("reply element", "reply elements")},
	},
	"dynamodb": {
		Networked: false, Check: dynamoCheck, New: newDynamoDB, Timeout: 60 * time.Second,
		Budget: Budget{Unit: "read capacity units", Example: "2.5", Parse: parseDecimal,
			Format: formatCount("read capacity unit", "read capacity units")},
	},
	"kafka": {
		Networked: true, Required: []string{"port"}, Check: kafkaCheck, Validate: kafkaValidate, New: newKafka,
		Budget: Budget{Unit: "records", Example: "1000", Parse: parseWhole, Format: formatCount("record", "records")},
	},
	"athena": {
		Networked: false, Required: []string{"workgroup"}, Check: athenaCheck, Validate: athenaValidate, New: newAthena,
		Budget: Budget{Unit: "bytes scanned", Example: "10GB", Parse: parseBytes, Format: formatBytes},
	},
}

func Names() []string {
	return slices.Sorted(maps.Keys(Kinds))
}

// Looks up the kind and checks the required keys
func Get(conn config.Conn) (Kind, error) {
	k, ok := Kinds[conn.Client]
	if !ok {
		return Kind{}, fmt.Errorf("unsupported client: %s\navailable: %s", conn.Client, strings.Join(Names(), " "))
	}
	var missing []string
	for _, key := range k.Required {
		if !conn.Has(key) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return Kind{}, fmt.Errorf("%s connection is missing: %s", conn.Client, strings.Join(missing, ", "))
	}
	if k.Validate != nil {
		if err := k.Validate(conn); err != nil {
			return Kind{}, err
		}
	}
	return k, nil
}

type Watching struct {
	Interval time.Duration
	// The field that tells polled records apart
	// Empty means the whole record
	Key string
}

// Records remembered to tell polled records apart
const seenMax = 100_000

// Keeps emitting new records
// A query the Follower handles uses the source's own way
// Anything else is polled
func Watch(ctx context.Context, c Client, q Query, w Watching, emit Emit) error {
	if f, ok := c.(Follower); ok {
		handled, err := f.Follow(ctx, q, emit)
		if handled {
			return err
		}
	}
	return poll(ctx, c, q, w, emit)
}

// One Read under q.Timeout
func Run(ctx context.Context, c Client, q Query, emit Emit) error {
	if q.Timeout <= 0 {
		return c.Read(ctx, q, emit)
	}
	run, cancel := context.WithTimeout(ctx, q.Timeout)
	defer cancel()
	err := c.Read(run, q, emit)
	if runPastDeadline(ctx, run, err) {
		return timeoutError(q.Timeout)
	}
	return err
}

// Only a deadline of this run becomes the timeout message
// 1. An ended request keeps its own error
// 2. Any other error keeps its own message even past the deadline such as a sink that failed
// 3. A driver that cuts a read by the socket deadline returns the os deadline error instead of the context's
func runPastDeadline(ctx, run context.Context, err error) bool {
	deadline := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
	return deadline && !pastDeadline(ctx) && pastDeadline(run)
}

// A socket deadline set from the context's deadline can fire just before the context's timer marks it done
// so the clock decides too
func pastDeadline(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	d, ok := ctx.Deadline()
	return ok && !time.Now().Before(d)
}

func timeoutError(d time.Duration) error {
	return fmt.Errorf("query ran past --timeout %s", d)
}

// Reruns the query every interval and emits only records not seen before
// It remembers a 16 byte hash per record and forgets the oldest past seenMax
func poll(ctx context.Context, c Client, q Query, w Watching, emit Emit) error {
	s := newSeen(seenMax)
	for {
		err := Run(ctx, c, q, func(r record.Record) error {
			if !s.add(r.Identity(w.Key)) {
				return nil
			}
			return emit(r)
		})
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.Interval):
		}
	}
}

// Hashes of seen records
// Keeps arrival order to drop the oldest first once past the limit
type seen struct {
	limit int
	set   map[[16]byte]struct{}
	order [][16]byte
	h     hash.Hash
}

func newSeen(limit int) *seen {
	// Only a size or a key past 64 bytes fails
	h, _ := blake2b.New(16, nil)
	return &seen{limit: limit, set: map[[16]byte]struct{}{}, h: h}
}

// Remembers the identity and reports whether it's new
func (s *seen) add(identity []byte) bool {
	s.h.Reset()
	s.h.Write(identity)
	var id [16]byte
	copy(id[:], s.h.Sum(nil))
	if _, ok := s.set[id]; ok {
		return false
	}
	s.set[id] = struct{}{}
	s.order = append(s.order, id)
	if len(s.order) > s.limit {
		delete(s.set, s.order[0])
		s.order = s.order[1:]
	}
	return true
}
