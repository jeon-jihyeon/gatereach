package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/record"
	"github.com/jeon-jihyeon/gatereach/internal/session"
	"github.com/jeon-jihyeon/gatereach/internal/sinks"
	"github.com/jeon-jihyeon/gatereach/internal/tunnels"
	"github.com/jeon-jihyeon/gatereach/internal/version"
)

const (
	defaultLimit    = 1000
	defaultInterval = 5 * time.Second
	// A shorter poll would turn --follow into a load on the source
	minInterval = time.Second
	// A query file past this is more likely data than a query
	maxFile = 1 << 20
)

// Seconds past this wrap around when turned into a Duration
// Whole seconds so the bound matches the error message
var maxSeconds = math.Floor(time.Duration(math.MaxInt64).Seconds())

type queryOptions struct {
	alias    string
	text     string
	write    bool
	limit    int
	out      string
	format   string
	follow   bool
	events   bool
	interval time.Duration
	key      string
	// Parsed only once the alias gives the source's unit
	max     string
	timeout time.Duration
	// Without --timeout the client kind's default applies
	timeoutSet bool
}

// The query is either the positional argument or --file
// A file is read only when named so a file named like the query never slips in
func (a *app) parseQuery(args []string) (queryOptions, error) {
	fs := newFlags("q", a.stderr)
	var o queryOptions
	file := fs.String("file", "", "file holding the query")
	fs.BoolVar(&o.write, "write", false, "allow writes")
	fs.IntVar(&o.limit, "limit", defaultLimit, "most records each run returns\neach polling run included")
	fs.StringVar(&o.out, "out", "", "save results as a Data Package folder")
	fs.StringVar(&o.format, "format", "arrow", "data file format for --out: "+strings.Join(sinks.FormatNames(), " or "))
	fs.BoolVar(&o.follow, "follow", false, "keep printing new records until stopped")
	fs.BoolVar(&o.events, "events", false, "wrap records and the final outcome as versioned JSONL events")
	interval := fs.Float64("interval", defaultInterval.Seconds(), "seconds between polls with --follow")
	fs.StringVar(&o.key, "key", "", "field that tells polled records apart\nthe whole record when left out")
	fs.StringVar(&o.max, "max", "",
		"most the source may read over the command in its own unit\nsuch as bytes scanned for athena or rows examined for mysql")
	timeout := fs.Float64("timeout", 0, "seconds each run may take\n0 for no limit\nthe client's default when left out")
	pos, err := parse(fs, args)
	if err != nil {
		return o, err
	}
	switch {
	case *file != "" && len(pos) == 1:
		text, err := readQueryFile(*file)
		if err != nil {
			return o, err
		}
		o.alias, o.text = pos[0], text
	case *file == "" && len(pos) == 2:
		o.alias, o.text = pos[0], pos[1]
	default:
		return o, usageError("q")
	}
	if o.events && o.out != "" {
		return o, errors.New("--events cannot be combined with --out")
	}
	if o.limit < 1 {
		return o, errors.New("--limit must be at least 1")
	}
	if _, ok := sinks.Formats[o.format]; !ok {
		return o, fmt.Errorf("unsupported format: %s\navailable: %s", o.format, strings.Join(sinks.FormatNames(), " "))
	}
	// The negated forms also refuse NaN
	if !(*interval >= minInterval.Seconds()) {
		return o, errors.New("--interval must be at least 1 second")
	}
	if !(*timeout >= 0) {
		return o, errors.New("--timeout must be 0 or above")
	}
	if o.interval, err = duration("interval", *interval); err != nil {
		return o, err
	}
	if o.timeout, err = duration("timeout", *timeout); err != nil {
		return o, err
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "timeout" {
			o.timeoutSet = true
		}
	})
	return o, nil
}

// 1. Only a regular file so a FIFO or a device never blocks or streams forever
// 2. Opened without blocking and checked on the open file so a path swapped after the check can't block either
// 3. At most 1 MiB so a large file the agent points at never goes out whole as a query
func readQueryFile(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", fmt.Errorf("couldn't read --file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("couldn't read --file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return "", errors.New("--file must be a regular file")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxFile+1))
	if err != nil {
		return "", fmt.Errorf("couldn't read --file: %w", err)
	}
	if len(b) > maxFile {
		return "", errors.New("--file is larger than 1 MiB")
	}
	return string(b), nil
}

func duration(name string, seconds float64) (time.Duration, error) {
	if seconds >= maxSeconds {
		return 0, fmt.Errorf("--%s must be under %d seconds", name, int64(maxSeconds))
	}
	d := time.Duration(seconds * float64(time.Second))
	// A positive value that truncates to zero would turn the limit off
	if seconds > 0 && d == 0 {
		return 0, fmt.Errorf("--%s must be at least one nanosecond", name)
	}
	return d, nil
}

func (a *app) query(args []string) error {
	o, err := a.parseQuery(args)
	if err != nil {
		return err
	}
	r := queryReport{Schema: "gatereach/v1", Type: "end", Version: version.Version, Alias: o.alias,
		Limit: o.limit, Follow: o.follow, Status: "finished"}
	interrupted, err := a.runQuery(o, &r)
	err = exitFor(o.follow, interrupted, err)
	switch {
	case interrupted && err == nil:
		r.Status = "stopped"
	case interrupted && errors.As(err, new(exitCode)):
		r.Status = "interrupted"
	case err != nil:
		r.Status = "error"
	case r.Session != nil && r.Session.Spent:
		r.Status = "budget"
	}
	if err != nil {
		r.Error = err.Error()
	}
	if o.events {
		if writeErr := writeJSON(a.stdout, r); writeErr != nil {
			var code exitCode
			if errors.As(err, &code) {
				return writeErr
			}
			return joinErrors(err, writeErr)
		}
	}
	return err
}

// 1. A follow stopped by a signal exits cleanly
// 2. A stopped single read is recorded as interrupted and exits 130
// 3. A failure to close the output wins over the interrupt and exits 1
func exitFor(follow, interrupted bool, err error) error {
	switch {
	case err != nil:
		return err
	case interrupted && !follow:
		return exitCode{code: exitInterrupted, msg: "interrupted"}
	}
	return nil
}

// The session owns the source and the Sink owns the output
// Only this function joins the two
// The write rules are checked before any tunnel or session opens
func (a *app) runQuery(o queryOptions, report *queryReport) (bool, error) {
	c, err := a.conn(o.alias)
	if err != nil {
		return false, err
	}
	// Only the user turns writes on for an alias
	// so an agent told to write by what it read can't do so through a read-only alias
	if o.write && !c.Write {
		return false, fmt.Errorf("%s doesn't allow --write\nset write = true for it in %s by hand", o.alias, config.Path())
	}
	kind, err := clients.Get(c)
	if err != nil {
		return false, err
	}
	if !o.timeoutSet {
		o.timeout = kind.Timeout
	}
	if err := kind.Check(o.text, o.write); err != nil {
		return false, err
	}
	maxAmount, err := parseMax(c.Client, kind.Budget, o.max)
	if err != nil {
		return false, err
	}
	if maxAmount > 0 {
		report.Budget = &queryBudget{Max: maxAmount, Unit: kind.Budget.Unit}
	}
	if err := sinks.CheckFolder(o.out); err != nil {
		return false, err
	}
	tunnel, started, err := a.prepare(o.alias, c, kind)
	if err != nil {
		return false, err
	}
	// conns.toml can carry terminal escapes in an alias
	name := clean(o.alias)
	if tunnel == tunnels.Opened || tunnel == tunnels.Foreign {
		fmt.Fprintf(a.stderr, "%s %s\n", name, tunnelMessages[tunnel])
	}
	if started {
		fmt.Fprintf(a.stderr, "%s session started\n", name)
	}
	// SIGTERM and a hangup wrap up the same way as Ctrl-C
	// Caught before the sink opens so a signal never leaves its output unclosed
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	meta := sinks.Meta{
		Version: version.Version, Alias: o.alias, Client: c.Client, User: c.User,
		Query: o.text, Limit: o.limit, Write: o.write, Follow: o.follow, Started: time.Now(),
	}
	sink, err := sinks.Get(o.out, o.format, meta, a.stdout, a.stderr)
	if err != nil {
		return false, err
	}
	req := session.Request{
		Version: version.Version, Alias: o.alias, Query: o.text, Write: o.write,
		Limit: o.limit, Follow: o.follow, Interval: o.interval, Key: o.key, Max: maxAmount, Timeout: o.timeout,
	}
	emit := writer(sink)
	end, runErr := a.send(ctx, req, func(line []byte) error {
		if o.events {
			line = append(append([]byte(`{"schema":"gatereach/v1","type":"record","record":`), line...), '}')
		}
		if err := emit(line); err != nil {
			return err
		}
		report.Rows++
		return nil
	})
	if runErr == nil || end.Error != "" {
		report.Session = &end
	}
	// Interrupted only when the cancel ended the run
	// 1. A signal after a finished run leaves its result as is
	// 2. An error the session sent stays a run error
	// 3. Closing the socket can cut a line so any other error after the cancel counts
	interrupted := runErr != nil && end.Error == "" && ctx.Err() != nil
	switch {
	case interrupted && o.follow:
		runErr = nil
	case interrupted:
		runErr = errors.New("interrupted")
	}
	if end.Reconnected {
		fmt.Fprintf(a.stderr, "%s reconnected after a drop\nsession variables and temporary tables were reset\n", name)
	}
	closeErr := sink.Close(runErr)
	// Only an End the session sent carries a total
	// 1. An interrupt or a broken connection leaves none
	// 2. Every failure the session sent has its error text
	if maxAmount > 0 && !interrupted && (runErr == nil || end.Error != "") {
		fmt.Fprintln(a.stderr, budgetReport(name, kind.Budget, maxAmount, end))
	}
	if interrupted {
		return true, closeErr
	}
	return false, joinErrors(runErr, closeErr)
}

// A failed sink write comes back from both the run and the close
// so the same error is reported once
func joinErrors(runErr, closeErr error) error {
	if closeErr != nil && errors.Is(runErr, closeErr) {
		return runErr
	}
	return errors.Join(runErr, closeErr)
}

// No --max is 0
func parseMax(client string, b clients.Budget, text string) (float64, error) {
	if text == "" {
		return 0, nil
	}
	amount, err := b.Parse(text)
	if err != nil {
		return 0, fmt.Errorf("%s --max takes %s, such as %s", client, b.Unit, b.Example)
	}
	return amount, nil
}

func budgetReport(alias string, b clients.Budget, maxAmount float64, end session.End) string {
	if end.Spent {
		return fmt.Sprintf("%s stopped at --max: read %s of %s", alias, b.Format(end.Used), b.Format(maxAmount))
	}
	return fmt.Sprintf("%s read %s of --max %s", alias, b.Format(end.Used), b.Format(maxAmount))
}

// A session of another version refuses before sending anything
// so it's replaced by one this binary starts and the request goes once more
func (a *app) send(ctx context.Context, req session.Request, emit func(line []byte) error) (session.End, error) {
	end, err := session.Query(ctx, req, emit)
	if !errors.Is(err, session.ErrOutdated) {
		return end, err
	}
	if _, err := session.Stop(req.Alias); err != nil {
		return session.End{}, err
	}
	if _, err := session.Ensure(req.Alias); err != nil {
		return session.End{}, err
	}
	fmt.Fprintf(a.stderr, "%s session restarted on the new version\n", clean(req.Alias))
	return session.Query(ctx, req, emit)
}

// stdout JSON lines copy the session's lines as is
// Only folder output parses them into records
func writer(sink sinks.Sink) func(line []byte) error {
	if raw, ok := sink.(sinks.RawWriter); ok {
		return raw.WriteRaw
	}
	return func(line []byte) error {
		r, err := record.FromJSON(line)
		if err != nil {
			return err
		}
		return sink.Write(r)
	}
}
