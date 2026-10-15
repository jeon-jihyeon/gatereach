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
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

const (
	testAlias   = "a"
	testVersion = "1.0.0"
	// 테스트 바이너리를 세션 대신 띄우는 도우미 모드
	helperEnv = "GATEREACH_TEST_SESSION"
)

// 도우미 모드
// 1. exit: 로그를 남기고 종료 코드 3
// 2. sleep: 소켓 없이 대기
// 3. deaf: 소켓을 열고 요청에 답하지 않음
func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "":
		os.Exit(m.Run())
	case "exit":
		fmt.Fprintln(os.Stderr, "연결 거부됨")
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

// New가 부를 때마다 cs 순서대로 mock을 하나씩 돌려주는 종류
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

// 세션 테스트는 GATEREACH_CONFIG와 clients.Kinds를 바꾸므로 병렬 실행하지 않음
func serveMocks(t *testing.T, busy time.Duration, check func(string, bool) error, cs ...*mocks.MockClient) {
	t.Helper()
	useConfig(t)
	clients.Kinds["mock"] = mockKind(check, cs...)
	t.Cleanup(func() { delete(clients.Kinds, "mock") })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, testAlias, testVersion, config.Conn{Client: "mock"}, busy) }()
	require.Eventually(t, func() bool { return Alive(testAlias) }, 2*time.Second, 10*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

// Connect와 Close를 받아 주는 mock
func connected(t *testing.T) *mocks.MockClient {
	t.Helper()
	c := mocks.NewMockClient(gomock.NewController(t))
	c.EXPECT().Connect(gomock.Any()).Return(nil)
	c.EXPECT().Close().Return(nil).AnyTimes()
	return c
}

// limit 건을 내보내는 Read
func emitRows(_ context.Context, q clients.Query, emit clients.Emit) error {
	for i := range q.Limit {
		if err := emit(record.New([]string{"i", "q"}, []any{int64(i), q.Text})); err != nil {
			return err
		}
	}
	return nil
}

// 버전을 비운 요청은 세션과 같은 버전
func query(ctx context.Context, req Request) ([]string, End, error) {
	if req.Version == "" {
		req.Version = testVersion
	}
	var lines []string
	end, err := Query(ctx, req, func(line []byte) error {
		lines = append(lines, string(line))
		return nil
	})
	return lines, end, err
}

func TestQuery(t *testing.T) {
	type want struct {
		lines       []string
		reconnected bool
		outdated    bool
		err         string
	}
	tcs := []struct {
		name  string
		req   Request
		check func(string, bool) error
		init  func(t *testing.T) []*mocks.MockClient
		want  want
	}{
		{"reuses one connection", Request{Alias: testAlias, Query: "x", Limit: 2}, nil, func(t *testing.T) []*mocks.MockClient {
			c := connected(t)
			c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows).Times(1)
			return []*mocks.MockClient{c}
		}, want{lines: []string{`{"i":0,"q":"x"}`, `{"i":1,"q":"x"}`}}},
		{"check refuses before reading", Request{Alias: testAlias, Query: "bad", Limit: 1}, func(string, bool) error { return errors.New("막힌 문장") },
			func(t *testing.T) []*mocks.MockClient { return []*mocks.MockClient{connected(t)} }, want{err: "막힌 문장"}},
		{"a dropped connection reconnects once", Request{Alias: testAlias, Query: "x", Limit: 1}, nil, func(t *testing.T) []*mocks.MockClient {
			old := connected(t)
			old.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(clients.ErrDisconnected)
			fresh := connected(t)
			fresh.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows)
			return []*mocks.MockClient{old, fresh}
		}, want{lines: []string{`{"i":0,"q":"x"}`}, reconnected: true}},
		{"another version is refused before reading", Request{Version: "0.9.0", Alias: testAlias, Query: "x", Limit: 1}, nil,
			func(t *testing.T) []*mocks.MockClient { return []*mocks.MockClient{connected(t)} },
			want{outdated: true, err: "a 세션 버전 1.0.0와 요청 버전 0.9.0가 다름"}},
		{"limit below 1 is refused", Request{Alias: testAlias, Query: "x"}, nil,
			func(t *testing.T) []*mocks.MockClient { return []*mocks.MockClient{connected(t)} }, want{err: "limit은 1 이상"}},
		{"follow without an interval is refused", Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true}, nil,
			func(t *testing.T) []*mocks.MockClient { return []*mocks.MockClient{connected(t)} }, want{err: "interval은 0보다 커야 함"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			serveMocks(t, time.Second, tc.check, tc.init(t)...)
			lines, end, err := query(context.Background(), tc.req)
			assert.Equal(t, tc.want.err == "", err == nil, "%v", err)
			assert.Contains(t, fmt.Sprint(err), tc.want.err)
			assert.Equal(t, tc.want.outdated, errors.Is(err, ErrOutdated))
			assert.Equal(t, tc.want.lines, lines)
			assert.Equal(t, len(tc.want.lines), end.Rows)
			assert.Equal(t, tc.want.reconnected, end.Reconnected)
		})
	}
}

// 소켓 이름이 겹쳐 다른 별칭의 요청이 와도 쿼리하지 않음
func TestAnotherAliasIsRefused(t *testing.T) {
	serveMocks(t, time.Second, nil, connected(t))
	c, err := Dial(testAlias)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	require.NoError(t, json.NewEncoder(c).Encode(Request{Alias: "a_b", Query: "x", Limit: 1}))
	var end End
	require.NoError(t, json.NewDecoder(c).Decode(&end))
	assert.Equal(t, "a 세션에 a_b 요청", end.Error)
}

// 재연결이 실패해도 세션은 살아 다음 요청이 이전 클라이언트로 다시 시도
func TestFailedReconnectKeepsServing(t *testing.T) {
	old := connected(t)
	gomock.InOrder(
		old.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(clients.ErrDisconnected),
		old.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows),
	)
	fresh := mocks.NewMockClient(gomock.NewController(t))
	fresh.EXPECT().Connect(gomock.Any()).Return(errors.New("터널 닫힘"))
	serveMocks(t, time.Second, nil, old, fresh)
	_, _, err := query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
	assert.ErrorContains(t, err, "재연결 실패")
	lines, _, err := query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1})
	require.NoError(t, err)
	assert.Len(t, lines, 1)
}

func TestBusySecondRead(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := connected(t)
	c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, q clients.Query, emit clients.Emit) error {
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
	assert.ErrorContains(t, err, "세션 사용 중")
	close(release)
	assert.NoError(t, <-first)
}

func TestFollowDoesNotBlockReads(t *testing.T) {
	following := make(chan struct{})
	held := connected(t)
	held.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(emitRows)
	follow := connected(t)
	follow.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, q clients.Query, emit clients.Emit) error {
		close(following)
		<-ctx.Done()
		return nil
	})
	serveMocks(t, 50*time.Millisecond, nil, held, follow)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _, _ = query(ctx, Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Millisecond})
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
	c.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
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

// follow가 도는 중에도 Stop이 세션을 끝냄
func TestStopEndsASessionWithAFollow(t *testing.T) {
	following := make(chan struct{})
	held := connected(t)
	follow := connected(t)
	follow.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
		close(following)
		<-ctx.Done()
		return nil
	})
	serveMocks(t, time.Second, nil, held, follow)
	go func() {
		_, _, _ = query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Millisecond})
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

// 거부해야 하는 소켓 폴더
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
	}, "실제 폴더가 아님"},
}

func TestEnsureDir_에러(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
			t.Cleanup(func() { _ = os.RemoveAll(Dir()) })
			tc.setup(t, Dir())
			assert.ErrorContains(t, ensureDir(), tc.err)
		})
	}
}

// 클라이언트도 연결 전에 폴더를 검사해 가로챈 폴더의 소켓에 붙지 않음
func TestDial_에러(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
			t.Cleanup(func() { _ = os.RemoveAll(Dir()) })
			tc.setup(t, Dir())
			_, err := Dial(testAlias)
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

// Accept가 종료 밖의 이유로 실패해도 follow를 취소하고 끝남
func TestAcceptFailureEndsAFollow(t *testing.T) {
	useConfig(t)
	following := make(chan struct{})
	// loop는 쥔 연결을 직접 쓰지 않아 호출 기대 없음
	held := mocks.NewMockClient(gomock.NewController(t))
	follow := connected(t)
	follow.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ clients.Query, _ clients.Emit) error {
		close(following)
		<-ctx.Done()
		return nil
	})
	require.NoError(t, ensureDir())
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: SocketPath(testAlias), Net: "unix"})
	require.NoError(t, err)
	s := &server{
		alias: testAlias, version: testVersion, kind: mockKind(nil, follow), client: held,
		busy: time.Second, turn: make(chan struct{}, 1), quit: make(chan struct{}), ln: ln,
	}
	done := make(chan error, 1)
	go func() { done <- s.loop(context.Background()) }()
	go func() {
		_, _, _ = query(context.Background(), Request{Alias: testAlias, Query: "x", Limit: 1, Follow: true, Interval: time.Millisecond})
	}()
	<-following
	_ = ln.Close()
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the loop kept waiting for the follow")
	}
}

// 연결과 비밀값 해석이 실패하면 소켓을 열지 않음
func TestServe_에러(t *testing.T) {
	tcs := []struct {
		name string
		conn config.Conn
		init func(t *testing.T) []*mocks.MockClient
		err  string
	}{
		{"connect fails", config.Conn{Client: "mock"}, func(t *testing.T) []*mocks.MockClient {
			c := mocks.NewMockClient(gomock.NewController(t))
			c.EXPECT().Connect(gomock.Any()).Return(errors.New("터널 닫힘"))
			return []*mocks.MockClient{c}
		}, "a 연결 실패: 터널 닫힘"},
		{"secret is missing", config.Conn{Client: "mock", Secret: "env:GATEREACH_TEST_MISSING"},
			func(*testing.T) []*mocks.MockClient { return nil }, "환경변수 없음"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			clients.Kinds["mock"] = mockKind(nil, tc.init(t)...)
			t.Cleanup(func() { delete(clients.Kinds, "mock") })
			err := serve(context.Background(), testAlias, testVersion, tc.conn, time.Second)
			assert.ErrorContains(t, err, tc.err)
			_, statErr := os.Stat(SocketPath(testAlias))
			assert.True(t, os.IsNotExist(statErr))
		})
	}
}

// 세션 쪽이 깨진 응답을 보내면 호출자가 원인을 알림
func TestQueryResponse_에러(t *testing.T) {
	tcs := []struct {
		name  string
		reply string
		want  string
	}{
		{"unreadable end line", "nope\n", "a 세션 응답 해석 실패"},
		{"cut record line", `{"record":` + "\n", "a 세션 응답 해석 실패: 잘린 레코드 줄"},
		{"closed before the end line", `{"record":{"i":0}}` + "\n", "a 세션이 응답 중에 끊김"},
		{"outdated session", `{"rows":0,"error":"다른 버전","outdated":true}` + "\n", "세션 버전이 다름: 다른 버전"},
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

// 세션 프로세스가 끝나거나 시간 안에 소켓을 열지 않으면 로그와 함께 실패
func TestStart_에러(t *testing.T) {
	tcs := []struct {
		name string
		mode string
		want []string
	}{
		{"exits before the socket opens", "exit", []string{"a 세션 시작 실패: 종료 코드 3", "연결 거부됨", "로그: "}},
		{"never opens the socket", "sleep", []string{"a 세션 시작 실패: 300ms 안에 연결되지 않음"}},
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

// shutdown에 답하지 않는 세션은 소켓 반대편 PID에 SIGTERM
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

// 폴더가 가로채졌으면 세션이 남았을 수 있어 없음으로 넘기지 않음
func TestStop_에러(t *testing.T) {
	for _, tc := range badDirs {
		t.Run(tc.name, func(t *testing.T) {
			useConfig(t)
			tc.setup(t, Dir())
			stopped, err := Stop(testAlias)
			assert.False(t, stopped)
			assert.ErrorContains(t, err, "a 세션 종료 실패: ")
		})
	}
}
