// Package session is the resident process per alias that holds the source connection and takes requests over a Unix socket
package session

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
	"github.com/jeon-jihyeon/gatereach/internal/record"
	"github.com/jeon-jihyeon/gatereach/internal/secrets"
)

const (
	startWait = 20 * time.Second
	startTick = 100 * time.Millisecond
	// How long a single read waits for its turn on the held connection
	busyWait = 30 * time.Second
	// How long a shutdown request waits for the socket to close
	stopWait = 5 * time.Second
	stopTick = 100 * time.Millisecond
	dialWait = time.Second
	// macOS sun_path is 104 bytes including the trailing NUL
	maxSocketPath = 103
	// Longest reply line the caller reads
	maxLine = 256 << 20
	// Longest request line the session reads
	// A query read from --file fits well under it
	maxRequest = 4 << 20
	// How long a new connection may take to send its request line
	requestWait = 10 * time.Second
	// Follows one session runs at once since each holds its own source connection
	maxFollows = 4
	// Shortest follow interval as the CLI also checks
	minInterval     = time.Second
	protocolVersion = "config1:"
)

type op string

const (
	opQuery    op = ""
	opShutdown op = "shutdown" // accepted from any version
)

// One request line
type Request struct {
	Op op `json:"op,omitempty"`
	// A version other than the session binary's is refused so a new binary takes the session over
	Version string `json:"version,omitempty"`
	Config  string `json:"config,omitempty"`
	// Checked so a query never reaches another alias's session even when socket names collide
	Alias    string        `json:"alias"`
	Query    string        `json:"query,omitempty"`
	Write    bool          `json:"write,omitempty"`
	Limit    int           `json:"limit,omitempty"`
	Follow   bool          `json:"follow,omitempty"`
	Interval time.Duration `json:"interval,omitempty"`
	Key      string        `json:"key,omitempty"`
	// --max already in the unit of the source's Budget
	// 0 means none
	Max     float64       `json:"max,omitempty"`
	Timeout time.Duration `json:"timeout,omitempty"`
}

// The last line of a reply
type End struct {
	Rows  int    `json:"rows"`
	Error string `json:"error,omitempty"`
	// The dropped connection was reopened so session variables and temporary tables were reset
	Reconnected bool `json:"reconnected,omitempty"`
	// The session version differs from the request's so nothing ran
	Outdated      bool `json:"outdated,omitempty"`
	ConfigChanged bool `json:"config_changed,omitempty"`
	// What the source read over the request when Max is set
	Used float64 `json:"used,omitempty"`
	// Used reached Max and ended the request
	Spent bool `json:"spent,omitempty"`
}

// The session runs on another binary version
// The caller restarts the session and sends the request again
var ErrOutdated = errors.New("session version differs")

var ErrConfigChanged = errors.New("connection configuration changed")

// Value ranges that also stop requests sent without the CLI
func (r Request) check() error {
	if r.Op != opQuery {
		return fmt.Errorf("unknown op: %s", r.Op)
	}
	if r.Limit < 1 {
		return errors.New("limit must be at least 1")
	}
	if r.Follow && r.Interval < minInterval {
		return errors.New("interval must be at least 1 second")
	}
	if r.Max < 0 {
		return errors.New("max must be 0 or above")
	}
	if r.Timeout < 0 {
		return errors.New("timeout must be 0 or above")
	}
	return nil
}

// The query a request runs and the meter its Use adds to
// The meter is nil without Max so the source measures nothing
func (r Request) query() (clients.Query, *meter) {
	q := clients.Query{Text: r.Query, Write: r.Write, Limit: r.Limit, Timeout: r.Timeout}
	if r.Max <= 0 {
		return q, nil
	}
	m := &meter{max: r.Max}
	q.Use = m.use
	return q, m
}

// What the source read over one request
// The source calls use from the request's goroutine only
type meter struct {
	max  float64
	used float64
}

func (m *meter) use(amount float64) error {
	m.used += amount
	if m.used >= m.max {
		return clients.ErrBudget
	}
	return nil
}

// A record line is `{"record":...}`
// Any other line is End
const recordPrefix = `{"record":`

// `run` under the config folder
// A folder per user and config folder elsewhere when that path would push socket names past the sun_path limit
// 1. $XDG_RUNTIME_DIR which only the user can enter
// 2. The temp folder when it isn't /tmp such as the per user one macOS keeps under /var/folders
// 3. /tmp last since any user can create a name there first
func Dir() string {
	d := filepath.Join(config.Dir(), "run")
	if fits(d) {
		return d
	}
	name := fmt.Sprintf("gatereach-%d-%08x", os.Getuid(), crc32.ChecksumIEEE([]byte(config.Dir())))
	for _, base := range []string{os.Getenv("XDG_RUNTIME_DIR"), os.TempDir()} {
		if !filepath.IsAbs(base) || filepath.Clean(base) == "/tmp" {
			continue
		}
		if d := filepath.Join(base, name); fits(d) {
			return d
		}
	}
	return filepath.Join("/tmp", name)
}

// Whether the shortest socket name still fits sun_path in folder d
func fits(d string) bool {
	return len(d)+len("/00000000.sock") <= maxSocketPath
}

// One socket per alias
// The alias hash keeps two aliases from sharing a file name
// Only the hash is used when the path would pass the sun_path limit
func SocketPath(alias string) string {
	p := filepath.Join(Dir(), config.FileKey(alias)+".sock")
	if len(p) <= maxSocketPath {
		return p
	}
	return filepath.Join(Dir(), config.AliasHash(alias)+".sock")
}

func lockPath(alias string) string {
	return SocketPath(alias) + ".lock"
}

func ensureDir() error {
	d := Dir()
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	return checkDir(d)
}

// Refuses
// 1. A symlink or a path that isn't a folder
// 2. An owner other than the current user
// 3. Any group or other permission
func checkDir(d string) error {
	fi, err := os.Lstat(d)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("session folder isn't a real folder: %s", d)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != os.Getuid() {
		return fmt.Errorf("session folder isn't owned by the current user: %s\nremove %s and try again", d, d)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("session folder permissions aren't 0700: %s", d)
	}
	return nil
}

// Connects only when the folder and the socket peer both belong to the current user
// so queries never go to a hijacked folder or another user's fake session
func dial(alias string) (*net.UnixConn, error) {
	if err := checkDir(Dir()); err != nil {
		return nil, err
	}
	c, err := net.DialTimeout("unix", SocketPath(alias), dialWait)
	if err != nil {
		return nil, err
	}
	uc := c.(*net.UnixConn)
	uid, err := peerUID(uc)
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("%s session socket peer couldn't be checked: %w", alias, err)
	}
	if uid != os.Getuid() {
		_ = uc.Close()
		return nil, fmt.Errorf("%s session socket peer isn't the current user", alias)
	}
	return uc, nil
}

func Alive(alias string) bool {
	c, err := dial(alias)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// Starts the session when there's none
// A flock on the lock file keeps one alias from starting twice at once
// The kernel releases it when the process ends so it's never stale state
func Ensure(alias string) (started bool, err error) {
	if Alive(alias) {
		return false, nil
	}
	if err := ensureDir(); err != nil {
		return false, err
	}
	unlock, err := proc.Lock(lockPath(alias))
	if err != nil {
		return false, err
	}
	defer unlock()
	if Alive(alias) {
		return false, nil
	}
	// Clears the socket a dead session left behind
	if err := os.Remove(SocketPath(alias)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, start(alias, startWait)
}

// Runs this executable as serve and waits until its socket accepts connections
func start(alias string, wait time.Duration) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	path := config.LogPath(alias, ".session.log")
	err = proc.Spawn([]string{exe, "serve", alias}, os.Environ(), path, func() bool { return Alive(alias) }, wait, startTick)
	var exit *proc.ExitError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &exit):
		return startError(alias, exit.Error(), path)
	case errors.Is(err, proc.ErrTimeout):
		return startError(alias, fmt.Sprintf("didn't connect within %s", wait), path)
	}
	return fmt.Errorf("%s session couldn't run: %w", alias, err)
}

func startError(alias, reason, path string) error {
	return fmt.Errorf("%s session failed to start: %s\n%s", alias, reason, proc.LogTail(path))
}

// 1. Sends SIGTERM to the socket peer when the socket outlasts wait after shutdown and the peer PID is unchanged
// 2. A missing folder or socket or a refused connection means no session and returns false
// 3. Any other connect failure is an error since the session may still be alive
func Stop(alias string) (bool, error) {
	return stop(alias, stopWait)
}

func stop(alias string, wait time.Duration) (bool, error) {
	c, err := dial(alias)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%s session wouldn't stop: %w", alias, err)
	}
	pid, _ := peerPID(c)
	deadline := time.Now().Add(wait)
	_ = json.NewEncoder(c).Encode(Request{Op: opShutdown, Alias: alias})
	_ = c.SetReadDeadline(deadline)
	_, _ = io.Copy(io.Discard, c)
	_ = c.Close()
	if waitGone(alias, deadline) {
		return true, nil
	}
	// The PID is read again so SIGTERM never reaches a new process that reused the PID of an ended session
	if pid > 0 && peerPIDNow(alias) == pid {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return false, fmt.Errorf("%s session wouldn't stop: %w", alias, err)
		}
	}
	if !waitGone(alias, time.Now().Add(wait)) {
		return false, fmt.Errorf("%s session wouldn't stop", alias)
	}
	return true, nil
}

// The socket peer's PID now
// 0 when the socket no longer accepts
func peerPIDNow(alias string) int {
	c, err := dial(alias)
	if err != nil {
		return 0
	}
	defer func() { _ = c.Close() }()
	pid, _ := peerPID(c)
	return pid
}

func waitGone(alias string, deadline time.Time) bool {
	for ; time.Now().Before(deadline); time.Sleep(stopTick) {
		if !Alive(alias) {
			return true
		}
	}
	return false
}

// Sends one request and passes the JSON of every record line to emit
// Ending ctx closes the connection so the session cancels the query
// 1. A failure the session reported comes back with its End
// 2. A failure on the way comes back with an empty End since no End arrived
func Query(ctx context.Context, req Request, emit func(line []byte) error) (End, error) {
	req.Version = protocolVersion + req.Version
	c, err := dial(req.Alias)
	if err != nil {
		return End{}, fmt.Errorf("%s couldn't reach the session: %w", req.Alias, err)
	}
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		if ctx.Err() != nil {
			return End{}, ctx.Err()
		}
		return End{}, fmt.Errorf("%s couldn't reach the session: %w", req.Alias, err)
	}
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.HasPrefix(line, []byte(recordPrefix)) {
			return parseEnd(req.Alias, line)
		}
		// A cut line can still end in a brace of the record's own value
		if !json.Valid(line) {
			return End{}, fmt.Errorf("%s couldn't parse the session's reply: truncated record line", req.Alias)
		}
		if err := emit(line[len(recordPrefix) : len(line)-1]); err != nil {
			return End{}, err
		}
	}
	if ctx.Err() != nil {
		return End{}, ctx.Err()
	}
	if err := sc.Err(); err != nil {
		return End{}, fmt.Errorf("%s couldn't read the session's reply: %w", req.Alias, err)
	}
	return End{}, fmt.Errorf("%s session dropped mid-response", req.Alias)
}

func parseEnd(alias string, line []byte) (End, error) {
	var end End
	if err := json.Unmarshal(line, &end); err != nil {
		return End{}, fmt.Errorf("%s couldn't parse the session's reply: %w", alias, err)
	}
	switch {
	case end.Outdated:
		return end, fmt.Errorf("%w: %s", ErrOutdated, end.Error)
	case end.ConfigChanged:
		return end, fmt.Errorf("%w: %s", ErrConfigChanged, end.Error)
	case end.Error != "":
		return end, errors.New(end.Error)
	}
	return end, nil
}

type server struct {
	alias string
	// Version of the binary that started the session
	version     string
	conn        config.Conn
	fingerprint string
	kind        clients.Kind
	password    string
	ln          *net.UnixListener

	// The one connection single reads use
	// The size 1 channel sets the turn and client is only swapped inside a turn
	turn   chan struct{}
	client clients.Client
	waits
	// A slot per running follow
	follows chan struct{}

	quit     chan struct{}
	quitOnce sync.Once
}

// How long the session waits on others
type waits struct {
	// For a turn on the held connection
	busy time.Duration
	// For the request line after accept
	request time.Duration
}

// Connects before opening the socket so a visible socket means it's ready
// The secret is resolved once here and reused by reconnects and follow connections
func Serve(ctx context.Context, alias, version string, conn config.Conn) error {
	return serve(ctx, alias, version, conn, waits{busy: busyWait, request: requestWait})
}

func serve(ctx context.Context, alias, version string, conn config.Conn, w waits) error {
	kind, err := clients.Get(conn)
	if err != nil {
		return err
	}
	if err := ensureDir(); err != nil {
		return err
	}
	password, err := secrets.Resolve(ctx, conn.Secret)
	if err != nil {
		return err
	}
	client := kind.New(conn, password)
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("%s connect failed: %w", alias, err)
	}
	s := &server{
		alias: alias, version: version, conn: conn, kind: kind, password: password, client: client,
		fingerprint: conn.Fingerprint(),
		waits:       w, turn: make(chan struct{}, 1), follows: make(chan struct{}, maxFollows), quit: make(chan struct{}),
	}
	defer func() { _ = s.client.Close() }()
	// bind takes no mode so the umask alone keeps the socket file private
	old := syscall.Umask(0o077)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: SocketPath(alias), Net: "unix"})
	syscall.Umask(old)
	if err != nil {
		return err
	}
	ln.SetUnlinkOnClose(true)
	s.ln = ln
	fmt.Printf("%s session ready %s\n", alias, SocketPath(alias))
	return s.loop(ctx)
}

// A shutdown or a stop signal also cancels running requests so a follow can't hold the session
// When Accept fails for another reason the requests are still canceled before waiting
func (s *server) loop(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-s.quit:
		}
		cancel()
		_ = s.ln.Close()
	}()
	for {
		c, err := s.ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		uid, err := peerUID(c)
		if err != nil || uid != os.Getuid() {
			_ = c.Close()
			continue
		}
		wg.Go(func() { s.handle(ctx, c) })
	}
}

func (s *server) shutdown() {
	s.quitOnce.Do(func() { close(s.quit) })
}

func (s *server) handle(parent context.Context, c *net.UnixConn) {
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Reads and writes on the connection don't watch ctx
	// so closing it is what ends a request line still on its way or a write to a sender that stopped reading
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 0, 64<<10), maxRequest)
	// A caller that connects and sends nothing would hold the connection for good
	_ = c.SetReadDeadline(time.Now().Add(s.request))
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			writeEnd(c, End{Error: fmt.Sprintf("%s session couldn't read the request: %s", s.alias, err)})
		}
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	var req Request
	if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
		writeEnd(c, End{Error: fmt.Sprintf("%s session couldn't parse the request: %s", s.alias, err)})
		return
	}
	if req.Alias != s.alias {
		writeEnd(c, End{Error: fmt.Sprintf("%s session got a request for %s", s.alias, req.Alias)})
		return
	}
	// Handled before the version check so a binary of another version can stop an old session
	if req.Op == opShutdown {
		writeEnd(c, End{})
		s.shutdown()
		return
	}
	if req.Version != protocolVersion+s.version {
		message := fmt.Sprintf("%s session version %s differs from request version %s",
			s.alias, s.version, strings.TrimPrefix(req.Version, protocolVersion))
		if !strings.HasPrefix(req.Version, protocolVersion) {
			message = fmt.Sprintf("%s session uses a different query protocol", s.alias)
		}
		writeEnd(c, End{Error: message, Outdated: true})
		return
	}
	if req.Config != s.fingerprint {
		writeEnd(c, End{
			Error: fmt.Sprintf("%s connection configuration changed\nclose %s before querying its new configuration",
				s.alias, s.alias),
			ConfigChanged: true,
		})
		return
	}
	// The query is canceled when the sender closes the connection
	go func() {
		_, _ = io.Copy(io.Discard, c)
		cancel()
	}()
	w := bufio.NewWriter(c)
	rows := 0
	emit := func(rec record.Record) error {
		_, _ = w.WriteString(recordPrefix)
		_, _ = w.Write(rec.JSON())
		// A bufio Writer keeps its first error so the last write reports a failure of any of them
		if _, err := w.WriteString("}\n"); err != nil {
			return err
		}
		rows++
		// Follow records are sparse so each one flushes at once
		// A single read flushes only when the buffer fills
		if req.Follow {
			return w.Flush()
		}
		return nil
	}
	end, err := s.runQuery(ctx, req, emit)
	// A sender that left reads nothing and a stopping session drops its requests
	if ctx.Err() != nil {
		return
	}
	end.Rows = rows
	if err != nil {
		end.Error = err.Error()
	}
	writeEnd(w, end)
	_ = w.Flush()
}

// Reaching --max is a normal end with Spent set
func (s *server) runQuery(ctx context.Context, req Request, emit clients.Emit) (End, error) {
	if err := req.check(); err != nil {
		return End{}, err
	}
	if req.Write && !s.conn.Write {
		return End{}, fmt.Errorf("%s doesn't allow --write", s.alias)
	}
	if err := s.kind.Check(req.Query, req.Write); err != nil {
		return End{}, err
	}
	q, m := req.query()
	var end End
	var err error
	if req.Follow {
		err = s.follow(ctx, q, clients.Watching{Interval: req.Interval, Key: req.Key}, emit)
	} else {
		end.Reconnected, err = s.read(ctx, q, emit)
	}
	if m != nil {
		end.Used = m.used
	}
	if errors.Is(err, clients.ErrBudget) {
		end.Spent, err = true, nil
	}
	return end, err
}

// 1. Single reads take turns on the held connection
// 2. A dropped connection is reopened and the read retried once
// 3. A failed reconnect keeps the old client so the next request tries again
func (s *server) read(ctx context.Context, q clients.Query, emit clients.Emit) (bool, error) {
	select {
	case s.turn <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(s.busy):
		return false, fmt.Errorf("%s session busy\nanother read still holds the connection", s.alias)
	}
	defer func() { <-s.turn }()
	err := clients.Run(ctx, s.client, q, emit)
	if !errors.Is(err, clients.ErrDisconnected) {
		return false, err
	}
	if err := s.checkPort(); err != nil {
		return false, err
	}
	fresh := s.kind.New(s.conn, s.password)
	if err := fresh.Connect(ctx); err != nil {
		return false, fmt.Errorf("%s reconnect failed: %w", s.alias, err)
	}
	_ = s.client.Close()
	s.client = fresh
	err = clients.Run(ctx, s.client, q, emit)
	if errors.Is(err, clients.ErrDisconnected) {
		return true, fmt.Errorf("%s connection dropped again after reconnecting", s.alias)
	}
	return true, err
}

// A follow uses its own connection for its lifetime so it never blocks single reads
// Follows are capped since each one holds a source connection and a poll loop
func (s *server) follow(ctx context.Context, q clients.Query, w clients.Watching, emit clients.Emit) error {
	select {
	case s.follows <- struct{}{}:
	default:
		return fmt.Errorf("%s session already runs %d follows", s.alias, maxFollows)
	}
	defer func() { <-s.follows }()
	if err := s.checkPort(); err != nil {
		return err
	}
	c := s.kind.New(s.conn, s.password)
	if err := c.Connect(ctx); err != nil {
		return fmt.Errorf("%s follow connect failed: %w", s.alias, err)
	}
	defer func() { _ = c.Close() }()
	return clients.Watch(ctx, c, q, w, emit)
}

// Swapped in tests to act as another user
var checkOwner = proc.CheckOwner

// A tunnel port another user bound after the tunnel dropped must not get the password
// A closed port is left to Connect so its failure keeps the plain cause
func (s *server) checkPort() error {
	if s.conn.Tunnel == nil {
		return nil
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(s.conn.Host, strconv.Itoa(s.conn.Port)), dialWait)
	if err != nil {
		return nil
	}
	_ = c.Close()
	return checkOwner(s.alias, s.conn.Port)
}

func writeEnd(w io.Writer, end End) {
	_ = json.NewEncoder(w).Encode(end)
}
