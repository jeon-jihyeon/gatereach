package session

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/jeon-jihyeon/gatereach/internal/clients"
	"github.com/jeon-jihyeon/gatereach/internal/clients/mocks"
	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const (
	testAlias   = "a"
	testVersion = "1.0.0"
	// Helper mode that runs the test binary in place of a session
	helperEnv = "GATEREACH_TEST_SESSION"
)

// Helper modes
// 1. exit: writes a log line and exits 3
// 2. sleep: waits without a socket
// 3. deaf: opens the socket and never answers
func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "":
		os.Exit(m.Run())
	case "exit":
		fmt.Fprintln(os.Stderr, "connection refused")
		os.Exit(3)
	case "sleep":
		time.Sleep(30 * time.Second)
	case "deaf":
		if err := ensureDir(); err != nil {
			os.Exit(4)
		}
		ln, err := net.Listen("unix", SocketPath(testAlias))
		if err != nil {
			os.Exit(5)
		}
		for {
			c, err := ln.Accept()
			if err != nil {
				os.Exit(6)
			}
			go func() { _, _ = io.Copy(io.Discard, c) }()
		}
	}
	os.Exit(0)
}

func useConfig(t *testing.T) {
	t.Helper()
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
	t.Cleanup(func() { _ = os.RemoveAll(Dir()) })
}

// A kind whose New hands out the mocks in cs one by one
func mockKind(check func(string, bool) error, cs ...*mocks.MockClient) clients.Kind {
	var mu sync.Mutex
	next := 0
	if check == nil {
		check = func(string, bool) error { return nil }
	}
	return clients.Kind{Check: check, New: func(config.Conn, string) clients.Client {
		mu.Lock()
		defer mu.Unlock()
		c := cs[next]
		next++
		return c
	}}
}

// Session tests don't run in parallel since they set GATEREACH_CONFIG and clients.Kinds
func serveMocks(t *testing.T, busy time.Duration, check func(string, bool) error, cs ...*mocks.MockClient) {
	t.Helper()
	serveConn(t, config.Conn{Client: "mock"}, waits{busy: busy, request: requestWait}, check, cs...)
}

func serveConn(t *testing.T, conn config.Conn, w waits, check func(string, bool) error, cs ...*mocks.MockClient) {
	t.Helper()
	useConfig(t)
	text, err := config.Render(testAlias, conn)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config.Path(), []byte(text), 0o600))
	clients.Kinds["mock"] = mockKind(check, cs...)
	t.Cleanup(func() { delete(clients.Kinds, "mock") })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, testAlias, testVersion, conn, w) }()
	require.Eventually(t, func() bool { return Alive(testAlias) }, 2*time.Second, 10*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

// Lets the mock accept Connect and Close
func accept(c *mocks.MockClient) *mocks.MockClient {
	c.EXPECT().Connect(gomock.Any()).Return(nil)
	c.EXPECT().Close().Return(nil).AnyTimes()
	return c
}

// A mock that accepts Connect and Close
func connected(t *testing.T) *mocks.MockClient {
	t.Helper()
	return accept(mocks.NewMockClient(gomock.NewController(t)))
}

func reads(c *mocks.MockClient) *gomock.Call {
	return c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any())
}

// nil for an empty msg so every query passes
func refuse(msg string) func(string, bool) error {
	if msg == "" {
		return nil
	}
	return func(string, bool) error { return errors.New(msg) }
}

// A Read that emits limit records
func emitRows(_ context.Context, q clients.Query, emit clients.Emit) error {
	for i := range q.Limit {
		if err := emit(record.New([]string{"i", "q"}, []any{int64(i), q.Text})); err != nil {
			return err
		}
	}
	return nil
}

// A request without a version gets the session's version
func query(ctx context.Context, req Request) ([]string, End, error) {
	if req.Version == "" {
		req.Version = testVersion
	}
	cfg, err := config.Load()
	if err != nil {
		return nil, End{}, err
	}
	req.Config = cfg.Conns[testAlias].Fingerprint()
	var lines []string
	end, err := Query(ctx, req, func(line []byte) error {
		lines = append(lines, string(line))
		return nil
	})
	return lines, end, err
}

func TestQuery(t *testing.T) {
	type args struct {
		req Request
		// A check that refuses every query with this message
		refuse string
	}
	type want struct {
		lines       []string
		reconnected bool
		outdated    bool
		err         string
	}
	one := Request{Alias: testAlias, Query: "x", Limit: 1}
	tcs := []struct {
		name string
		args args
		init func(held, fresh *mocks.MockClient)
		want want
	}{
		{"reuses one connection", args{req: Request{Alias: testAlias, Query: "x", Limit: 2}},
			func(held, _ *mocks.MockClient) { reads(accept(held)).DoAndReturn(emitRows).Times(1) },
			want{lines: []string{`{"i":0,"q":"x"}`, `{"i":1,"q":"x"}`}}},
		{"check refuses before reading", args{req: one, refuse: "blocked statement"},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "blocked statement"}},
		{"a dropped connection reconnects once", args{req: one}, func(held, fresh *mocks.MockClient) {
			reads(accept(held)).Return(clients.ErrDisconnected)
			reads(accept(fresh)).DoAndReturn(emitRows)
		}, want{lines: []string{`{"i":0,"q":"x"}`}, reconnected: true}},
		{"another version is refused before reading",
			args{req: Request{Version: "0.9.0", Alias: testAlias, Query: "x", Limit: 1}},
			func(held, _ *mocks.MockClient) { accept(held) },
			want{outdated: true, err: "a session version 1.0.0 differs from request version 0.9.0"}},
		{"limit below 1 is refused", args{req: Request{Alias: testAlias, Query: "x"}},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "limit must be at least 1"}},
		{"follow without an interval is refused",
			args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true}},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "interval must be at least 1 second"}},
		{"follow under 1 second is refused",
			args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second / 2}},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "interval must be at least 1 second"}},
		{"a negative max is refused", args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Max: -1}},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "max must be 0 or above"}},
		{"a negative timeout is refused",
			args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Timeout: -time.Second}},
			func(held, _ *mocks.MockClient) { accept(held) }, want{err: "timeout must be 0 or above"}},
		{"a read past the timeout ends with the timeout",
			args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Timeout: 20 * time.Millisecond}},
			func(held, _ *mocks.MockClient) {
				reads(accept(held)).DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
					<-ctx.Done()
					return ctx.Err()
				})
			}, want{err: "query ran past --timeout 20ms"}},
		{"a second drop after reconnecting is reported", args{req: one}, func(held, fresh *mocks.MockClient) {
			reads(accept(held)).Return(clients.ErrDisconnected)
			reads(accept(fresh)).Return(clients.ErrDisconnected)
		}, want{err: "a connection dropped again after reconnecting", reconnected: true}},
		{"a follow that can't connect is reported",
			args{req: Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second}},
			func(held, fresh *mocks.MockClient) {
				accept(held)
				fresh.EXPECT().Connect(gomock.Any()).Return(errors.New("tunnel closed"))
			}, want{err: "a follow connect failed: tunnel closed"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			held, fresh := mocks.NewMockClient(ctrl), mocks.NewMockClient(ctrl)
			tc.init(held, fresh)
			serveMocks(t, time.Second, refuse(tc.args.refuse), held, fresh)
			lines, end, err := query(context.Background(), tc.args.req)
			assert.Equal(t, tc.want.err == "", err == nil, "%v", err)
			assert.Contains(t, fmt.Sprint(err), tc.want.err)
			assert.Equal(t, tc.want.outdated, errors.Is(err, ErrOutdated))
			assert.Equal(t, tc.want.lines, lines)
			assert.Equal(t, len(tc.want.lines), end.Rows)
			assert.Equal(t, tc.want.reconnected, end.Reconnected)
		})
	}
}

// A Read that emits limit records and pays 1 after each one
func paidRows(_ context.Context, q clients.Query, emit clients.Emit) error {
	for i := range q.Limit {
		if err := emit(record.New([]string{"i"}, []any{int64(i)})); err != nil {
			return err
		}
		if q.Use == nil {
			continue
		}
		if err := q.Use(1); err != nil {
			return err
		}
	}
	return nil
}

func TestBudget(t *testing.T) {
	type want struct {
		rows  int
		used  float64
		spent bool
	}
	tcs := []struct {
		name string
		req  Request
		init func(held, fresh *mocks.MockClient)
		want want
	}{
		{"a read stops at max with the paid record", Request{Alias: testAlias, Query: "x", Limit: 5, Max: 2},
			func(held, _ *mocks.MockClient) { reads(accept(held)).DoAndReturn(paidRows) },
			want{rows: 2, used: 2, spent: true}},
		{"a read under max reports what it used", Request{Alias: testAlias, Query: "x", Limit: 3, Max: 10},
			func(held, _ *mocks.MockClient) { reads(accept(held)).DoAndReturn(paidRows) },
			want{rows: 3, used: 3}},
		{"without max nothing is measured", Request{Alias: testAlias, Query: "x", Limit: 3},
			func(held, _ *mocks.MockClient) {
				reads(accept(held)).DoAndReturn(func(ctx context.Context, q clients.Query, emit clients.Emit) error {
					if q.Use != nil {
						return errors.New("measured without max")
					}
					return paidRows(ctx, q, emit)
				})
			}, want{rows: 3}},
		{"a reconnected read keeps one meter", Request{Alias: testAlias, Query: "x", Limit: 5, Max: 2},
			func(held, fresh *mocks.MockClient) {
				reads(accept(held)).Return(clients.ErrDisconnected)
				reads(accept(fresh)).DoAndReturn(paidRows)
			}, want{rows: 2, used: 2, spent: true}},
		{"a follow counts every run until max",
			Request{Alias: testAlias, Query: "x", Limit: 2, Max: 4, Follow: true, Interval: time.Second},
			func(held, fresh *mocks.MockClient) {
				accept(held)
				reads(accept(fresh)).DoAndReturn(paidRows).Times(2)
			}, want{rows: 2, used: 4, spent: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			held, fresh := mocks.NewMockClient(ctrl), mocks.NewMockClient(ctrl)
			tc.init(held, fresh)
			serveMocks(t, time.Second, nil, held, fresh)
			lines, end, err := query(context.Background(), tc.req)
			require.NoError(t, err)
			assert.Equal(t, tc.want, want{rows: end.Rows, used: end.Used, spent: end.Spent})
			assert.Len(t, lines, tc.want.rows)
		})
	}
}

// A request line the session can't take runs nothing and still gets an End
// Another alias reaches this socket only through a name collision
func TestRequest_Errors(t *testing.T) {
	tcs := []struct {
		name string
		line string
		want string
	}{
		{"another alias", `{"alias":"a_b","query":"x","limit":1}`, "a session got a request for a_b"},
		{"unreadable request", "nope", "a session couldn't parse the request: invalid character"},
		{"old query protocol", `{"version":"1.0.0","alias":"a","query":"x","limit":1}`,
			"a session uses a different query protocol"},
		{"unknown op", fmt.Sprintf(`{"op":"drop","version":%q,"config":%q,"alias":"a","query":"x","limit":1}`,
			protocolVersion+testVersion, (config.Conn{Client: "mock"}).Fingerprint()), "unknown op: drop"},
		{"request past 4 MiB", strings.Repeat("x", maxRequest+1),
			"a session couldn't read the request: bufio.Scanner: token too long"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			serveMocks(t, time.Second, nil, connected(t))
			c, err := dial(testAlias)
			require.NoError(t, err)
			defer func() { _ = c.Close() }()
			// The session stops reading a line that is too long so the rest of the write may fail
			go func() { _, _ = io.WriteString(c, tc.line+"\n") }()
			var end End
			require.NoError(t, json.NewDecoder(c).Decode(&end))
			assert.Contains(t, end.Error, tc.want)
		})
	}
}

// The session survives a failed reconnect and the next request retries with the old client
func TestFailedReconnectKeepsServing(t *testing.T) {
	old := connected(t)
	gomock.InOrder(
		old.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(clients.ErrDisconnected),
		old.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows),
	)
	fresh := mocks.NewMockClient(gomock.NewController(t))
	fresh.EXPECT().Connect(gomock.Any()).Return(errors.New("tunnel closed"))
	serveMocks(t, time.Second, nil, old, fresh)
	_, _, err := query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
	assert.ErrorContains(t, err, "reconnect failed")
	lines, _, err := query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
	require.NoError(t, err)
	assert.Len(t, lines, 1)
}

func TestBusySecondRead(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := connected(t)
	c.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, q clients.Query, emit clients.Emit) error {
			close(entered)
			<-release
			return emitRows(ctx, q, emit)
		})
	serveMocks(t, 50*time.Millisecond, nil, c)
	first := make(chan error, 1)
	go func() {
		_, _, err := query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
		first <- err
	}()
	<-entered
	_, _, err := query(context.Background(), Request{Alias: testAlias, Query: "y", Limit: 1})
	assert.ErrorContains(t, err, "session busy")
	close(release)
	assert.NoError(t, <-first)
}

func TestFollowDoesNotBlockReads(t *testing.T) {
	following := make(chan struct{})
	held := connected(t)
	held.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows)
	follow := connected(t)
	follow.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, q clients.Query, emit clients.Emit) error {
			close(following)
			<-ctx.Done()
			return nil
		})
	serveMocks(t, 50*time.Millisecond, nil, held, follow)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _, _ = query(ctx, Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second})
		close(done)
	}()
	<-following
	lines, _, err := query(context.Background(), Request{Alias: testAlias, Query: "y", Limit: 1})
	require.NoError(t, err)
	assert.Len(t, lines, 1)
	cancel()
	<-done
}

func TestClosingTheClientCancelsTheQuery(t *testing.T) {
	canceled := make(chan struct{})
	c := connected(t)
	c.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
			<-ctx.Done()
			close(canceled)
			return ctx.Err()
		})
	serveMocks(t, time.Second, nil, c)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := query(ctx, Request{Alias: testAlias, Query: "x", Limit: 1})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("session kept the query running")
	}
}

// Stop ends the session even while a follow runs
func TestStopEndsASessionWithAFollow(t *testing.T) {
	following := make(chan struct{})
	held := connected(t)
	follow := connected(t)
	follow.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
			close(following)
			<-ctx.Done()
			return nil
		})
	serveMocks(t, time.Second, nil, held, follow)
	go func() {
		_, _, _ = query(
			context.Background(),
			Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second},
		)
	}()
	<-following
	stopped, err := Stop(testAlias)
	require.NoError(t, err)
	assert.True(t, stopped)
	assert.False(t, Alive(testAlias))
	_, err = os.Stat(SocketPath(testAlias))
	assert.True(t, os.IsNotExist(err))
	stopped, err = Stop(testAlias)
	require.NoError(t, err)
	assert.False(t, stopped)
}

// Stop ends the session even while it's stuck writing to a sender that stopped reading
// as a pager that quit leaves it
func TestStopEndsASessionStuckWriting(t *testing.T) {
	ended := make(chan struct{})
	wrote := make(chan error, 1)
	held := mocks.NewMockClient(gomock.NewController(t))
	held.EXPECT().Connect(gomock.Any()).Return(nil)
	held.EXPECT().Close().DoAndReturn(func() error {
		close(ended)
		return nil
	})
	big := strings.Repeat("x", 1<<20)
	reads(held).DoAndReturn(func(_ context.Context, _ clients.Query, emit clients.Emit) error {
		for {
			if err := emit(record.New([]string{"x"}, []any{big})); err != nil {
				wrote <- err
				return err
			}
		}
	})
	serveMocks(t, time.Second, nil, held)
	c, err := dial(testAlias)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, json.NewEncoder(c).Encode(Request{
		Version: protocolVersion + testVersion, Config: (config.Conn{Client: "mock"}).Fingerprint(),
		Alias: testAlias, Query: "x", Limit: 1,
	}))
	// Request delivery alone does not establish that Read reached the socket write
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	prefix := make([]byte, len(recordPrefix))
	_, err = io.ReadFull(c, prefix)
	require.NoError(t, err)
	require.Equal(t, recordPrefix, string(prefix))
	select {
	case err := <-wrote:
		t.Fatalf("write ended before shutdown: %v", err)
	default:
	}
	stopped, err := Stop(testAlias)
	require.NoError(t, err)
	assert.True(t, stopped)
	select {
	case <-ended:
		assert.Error(t, <-wrote)
	case <-time.After(2 * time.Second):
		t.Fatal("the session kept waiting on its write")
	}
}

// Stop ends the session even while a connection still owes its request line
func TestStopEndsASessionWaitingForARequest(t *testing.T) {
	ended := make(chan struct{})
	held := mocks.NewMockClient(gomock.NewController(t))
	held.EXPECT().Connect(gomock.Any()).Return(nil)
	held.EXPECT().Close().DoAndReturn(func() error {
		close(ended)
		return nil
	})
	serveConn(t, config.Conn{Client: "mock"}, waits{busy: time.Second, request: time.Minute}, nil, held)
	c, err := dial(testAlias)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	stopped, err := Stop(testAlias)
	require.NoError(t, err)
	assert.True(t, stopped)
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("the session kept waiting for the request line")
	}
}

// Socket folders that must be refused
var badDirs = []struct {
	name  string
	setup func(t *testing.T, dir string)
	err   string
}{
	{"loose permissions", func(t *testing.T, dir string) {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o755))
	}, "0700"},
	{"symlink", func(t *testing.T, dir string) {
		real := filepath.Join(t.TempDir(), "real")
		require.NoError(t, os.Mkdir(real, 0o700))
		require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
		require.NoError(t, os.Symlink(real, dir))
	}, "isn't a real folder"},
}

func TestEnsureDir_Errors(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			tc.setup(t, Dir())
			assert.ErrorContains(t, ensureDir(), tc.err)
		})
	}
}

// The client also checks the folder before connecting so it never joins a socket in a hijacked folder
func TestDial_Errors(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			tc.setup(t, Dir())
			_, err := dial(testAlias)
			assert.ErrorContains(t, err, tc.err)
		})
	}
}

func TestSocketPath(t *testing.T) {
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
	assert.NotEqual(t, SocketPath("a b"), SocketPath("a_b"), "aliases that share a file name get different sockets")
	long := SocketPath(strings.Repeat("x", 200))
	assert.LessOrEqual(t, len(long), maxSocketPath)
	assert.True(t, strings.HasSuffix(long, ".sock"))
}

// A config path too long for sockets moves the folder to the first short per user place
func TestDirFallback(t *testing.T) {
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), strings.Repeat("x", 100), "conns.toml"))
	name := fmt.Sprintf("gatereach-%d-", os.Getuid())
	tcs := []struct {
		name    string
		runtime string
		tmp     string
		want    string
	}{
		{"runtime folder first", "/run/user/1000", "/var/folders/x/T", "/run/user/1000/" + name},
		{"temp folder when it isn't /tmp", "", "/var/folders/x/T/", "/var/folders/x/T/" + name},
		{"relative runtime folder is skipped", "run", "/tmp", "/tmp/" + name},
		{"too long folders are skipped", "/" + strings.Repeat("r", 100), "/" + strings.Repeat("t", 100), "/tmp/" + name},
		{"/tmp last", "", "/tmp", "/tmp/" + name},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", tc.runtime)
			t.Setenv("TMPDIR", tc.tmp)
			assert.True(t, strings.HasPrefix(Dir(), tc.want), "%s", Dir())
		})
	}
}

// An Accept failure other than shutdown still cancels the follow and ends
func TestAcceptFailureEndsAFollow(t *testing.T) {
	useConfig(t)
	conn := config.Conn{Client: "mock"}
	text, err := config.Render(testAlias, conn)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config.Path(), []byte(text), 0o600))
	following := make(chan struct{})
	// loop never uses the held connection itself so no calls are expected
	held := mocks.NewMockClient(gomock.NewController(t))
	follow := connected(t)
	follow.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
			close(following)
			<-ctx.Done()
			return nil
		})
	require.NoError(t, ensureDir())
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: SocketPath(testAlias), Net: "unix"})
	require.NoError(t, err)
	s := &server{
		alias: testAlias, version: testVersion, kind: mockKind(nil, follow), client: held,
		conn: conn, fingerprint: conn.Fingerprint(),
		waits: waits{busy: time.Second, request: time.Second}, turn: make(chan struct{}, 1),
		follows: make(chan struct{}, maxFollows), quit: make(chan struct{}), ln: ln,
	}
	done := make(chan error, 1)
	go func() { done <- s.loop(context.Background()) }()
	go func() {
		_, _, _ = query(
			context.Background(),
			Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second},
		)
	}()
	select {
	case <-following:
	case <-time.After(2 * time.Second):
		_ = ln.Close()
		t.Fatal("the follow didn't start")
	}
	_ = ln.Close()
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the loop kept waiting for the follow")
	}
}

// A failed connect or secret lookup never opens the socket
func TestServe_Errors(t *testing.T) {
	tcs := []struct {
		name string
		conn config.Conn
		init func(t *testing.T) []*mocks.MockClient
		err  string
	}{
		{"connect fails", config.Conn{Client: "mock"}, func(t *testing.T) []*mocks.MockClient {
			c := mocks.NewMockClient(gomock.NewController(t))
			c.EXPECT().Connect(gomock.Any()).Return(errors.New("tunnel closed"))
			return []*mocks.MockClient{c}
		}, "a connect failed: tunnel closed"},
		{"secret is missing", config.Conn{Client: "mock", Secret: "env:GATEREACH_TEST_MISSING"},
			func(*testing.T) []*mocks.MockClient { return nil }, "no environment variable"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			clients.Kinds["mock"] = mockKind(nil, tc.init(t)...)
			t.Cleanup(func() { delete(clients.Kinds, "mock") })
			err := serve(context.Background(), testAlias, testVersion, tc.conn, waits{busy: time.Second, request: time.Second})
			assert.ErrorContains(t, err, tc.err)
			_, statErr := os.Stat(SocketPath(testAlias))
			assert.True(t, os.IsNotExist(statErr))
		})
	}
}

// A broken reply from the session tells the caller what went wrong
func TestQueryResponse_Errors(t *testing.T) {
	tcs := []struct {
		name  string
		reply string
		want  string
	}{
		{"unreadable end line", "nope\n", "a couldn't parse the session's reply"},
		{"cut record line", `{"record":` + "\n", "a couldn't parse the session's reply: truncated record line"},
		{"record line cut after an inner brace", `{"record":{"i":{"j":0}}` + "\n",
			"a couldn't parse the session's reply: truncated record line"},
		{"closed before the end line", `{"record":{"i":0}}` + "\n", "a session dropped mid-response"},
		{"outdated session", `{"rows":0,"error":"other version","outdated":true}` + "\n",
			"session version differs: other version"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			require.NoError(t, ensureDir())
			ln, err := net.Listen("unix", SocketPath(testAlias))
			require.NoError(t, err)
			t.Cleanup(func() { _ = ln.Close() })
			go func() {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				_, _ = bufio.NewReader(c).ReadBytes('\n')
				_, _ = io.WriteString(c, tc.reply)
			}()
			_, _, err = query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

// A session process that exits or doesn't open its socket in time fails with its log
func TestStart_Errors(t *testing.T) {
	tcs := []struct {
		name string
		mode string
		want []string
	}{
		{"exits before the socket opens", "exit",
			[]string{"a session failed to start: exit code 3", "connection refused", "log: "}},
		{"never opens the socket", "sleep", []string{"a session failed to start: didn't connect within 300ms\nlog: "}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			t.Setenv(helperEnv, tc.mode)
			err := start(testAlias, 300*time.Millisecond)
			for _, w := range tc.want {
				assert.ErrorContains(t, err, w)
			}
		})
	}
}

// A session that ignores shutdown gets SIGTERM at the socket peer's PID
func TestStopFallsBackToSIGTERM(t *testing.T) {
	useConfig(t)
	deaf := exec.Command(os.Args[0])
	deaf.Env = append(os.Environ(), helperEnv+"=deaf")
	require.NoError(t, deaf.Start())
	t.Cleanup(func() { _ = deaf.Process.Kill() })
	require.Eventually(t, func() bool { return Alive(testAlias) }, 5*time.Second, 20*time.Millisecond)
	stopped, err := stop(testAlias, 300*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, stopped)
	assert.Error(t, deaf.Wait(), "terminated by SIGTERM")
	assert.False(t, Alive(testAlias))
}

// A hijacked folder may still hide a session so it isn't treated as none
func TestStop_Errors(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			tc.setup(t, Dir())
			stopped, err := Stop(testAlias)
			assert.False(t, stopped)
			assert.ErrorContains(t, err, "a session wouldn't stop: ")
		})
	}
}

// A connection that sends no request line in time gets an End and is closed
func TestRequestDeadline(t *testing.T) {
	serveConn(t, config.Conn{Client: "mock"}, waits{busy: time.Second, request: 50 * time.Millisecond}, nil, connected(t))
	c, err := dial(testAlias)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	var end End
	require.NoError(t, json.NewDecoder(c).Decode(&end))
	assert.Contains(t, end.Error, "a session couldn't read the request: ")
	assert.Contains(t, end.Error, "i/o timeout")
}

// A fifth follow is refused while four run and a read still gets through
func TestFollowLimit(t *testing.T) {
	following := make(chan struct{}, maxFollows)
	held := connected(t)
	reads(held).DoAndReturn(emitRows)
	cs := []*mocks.MockClient{held}
	for range maxFollows {
		f := connected(t)
		reads(f).DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
			following <- struct{}{}
			<-ctx.Done()
			return nil
		})
		cs = append(cs, f)
	}
	serveMocks(t, time.Second, nil, cs...)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	follow := Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second}
	for range maxFollows {
		wg.Go(func() { _, _, _ = query(ctx, follow) })
	}
	for range maxFollows {
		<-following
	}
	_, _, err := query(context.Background(), follow)
	assert.EqualError(t, err, "a session already runs 4 follows")
	lines, _, err := query(context.Background(), Request{Alias: testAlias, Query: "y", Limit: 1})
	require.NoError(t, err)
	assert.Len(t, lines, 1)
}

// A tunnel port another user took over never gets a reconnect or a follow connection
func TestOwnerCheckBeforeConnecting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	held := errors.New("a port 1 is held by another user\nclose that listener or change the port")
	checkOwner = func(string, int) error { return held }
	t.Cleanup(func() { checkOwner = proc.CheckOwner })
	tcs := []struct {
		name string
		req  Request
		init func(c *mocks.MockClient)
	}{
		{"reconnect", Request{Alias: testAlias, Query: "x", Limit: 1},
			func(c *mocks.MockClient) { reads(c).Return(clients.ErrDisconnected) }},
		{"follow", Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Second},
			func(*mocks.MockClient) {}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := connected(t)
			tc.init(c)
			conn := config.Conn{
				Client: "mock", Host: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port, Tunnel: &config.Tunnel{Type: "tsh"},
			}
			serveConn(t, conn, waits{busy: time.Second, request: time.Second}, nil, c)
			_, _, err := query(context.Background(), tc.req)
			assert.EqualError(t, err, held.Error())
		})
	}
}
