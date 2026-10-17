package tunnels

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
)

const (
	testWait = time.Second
	testTick = 20 * time.Millisecond
)

// A port that stays open for the test
func listen(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

// A port nothing listens on
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return p
}

// Helper mode that runs the test binary in place of a tunnel
// It holds the port named in the value open for 2 seconds
const listenEnv = "GATEREACH_TEST_LISTEN"

func TestMain(m *testing.M) {
	if port := os.Getenv(listenEnv); port != "" {
		ln, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			os.Exit(3)
		}
		time.Sleep(2 * time.Second)
		_ = ln.Close()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Command line and environment of a helper child that opens the port
func listener(port int) ([]string, []string) {
	return []string{os.Args[0]}, append(os.Environ(), listenEnv+"="+strconv.Itoa(port))
}

func conn(port int) config.Conn {
	return config.Conn{
		Client: "mysql",
		Host:   "127.0.0.1",
		Port:   port,
		User:   "u",
		Tunnel: &config.Tunnel{Type: "tsh", Target: "t"},
	}
}

// What the owner check returns for a port of another user
var errOtherUser = errors.New("a port 1 is held by another user\nclose that listener or change the port")

// Not parallel since it sets GATEREACH_CONFIG and swaps checkOwner
func TestOpen(t *testing.T) {
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
	listening := listen(t)
	type want struct {
		result Result
		err    string
	}
	// A nil init means the connection has no tunnel
	// A non-nil owner is what the owner check returns
	tcs := []struct {
		name  string
		port  int
		init  func(k *MockKind, port int)
		owner error
		want  want
	}{
		{"our tunnel holds the port", listening, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{1}, nil)
		}, nil, want{result: Already}},
		{"another program holds the port", listening, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, nil, want{result: Foreign}},
		{"another user holds the port", listening, func(*MockKind, int) {}, errOtherUser, want{err: errOtherUser.Error()}},
		{"process list fails", listening, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, errors.New("ps failed"))
		}, nil, want{err: "ps failed"}},
		{"direct connection skips the owner check", listening, nil, errOtherUser, want{result: Already}},
		{"closed port without a tunnel", freePort(t), nil, nil, want{err: "a is closed with no tunnel configured"}},
		{"starts and waits for the port", freePort(t), func(k *MockKind, port int) {
			k.EXPECT().Command(gomock.Any()).Return(listener(port))
		}, nil, want{result: Opened}},
		{"another user holds the port the tunnel opened", freePort(t), func(k *MockKind, port int) {
			k.EXPECT().Command(gomock.Any()).Return(listener(port))
		}, errOtherUser, want{err: errOtherUser.Error()}},
		{"early exit shows the log", freePort(t), func(k *MockKind, _ int) {
			k.EXPECT().
				Command(gomock.Any()).
				Return([]string{"sh", "-c", "echo 'ERROR: not logged in' >&2; exit 3"}, os.Environ())
		}, nil, want{err: "a open failed: tunnel exit code 3\nERROR: not logged in"}},
		{"timeout asks to check the login", freePort(t), func(k *MockKind, _ int) {
			k.EXPECT().Command(gomock.Any()).Return([]string{"sleep", "30"}, os.Environ())
		}, nil, want{err: "check the tunnel login"}},
		{"missing tunnel program", freePort(t), func(k *MockKind, _ int) {
			k.EXPECT().Command(gomock.Any()).Return([]string{filepath.Join(t.TempDir(), "no-tsh")}, os.Environ())
		}, nil, want{err: "a tunnel couldn't run"}},
	}
	t.Cleanup(func() { checkOwner = proc.CheckOwner })
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			checkOwner = proc.CheckOwner
			if tc.owner != nil {
				checkOwner = func(string, int) error { return tc.owner }
			}
			var k Kind
			if tc.init != nil {
				m := NewMockKind(gomock.NewController(t))
				tc.init(m, tc.port)
				k = m
			}
			got, err := open(k, "a", conn(tc.port), testWait, testTick)
			assert.Equal(t, tc.want.result, got)
			assert.Equal(t, tc.want.err == "", err == nil, "error: %v", err)
			assert.Contains(t, errText(err), tc.want.err)
		})
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Concurrent opens of a closed alias start one tunnel and the rest find it already open
func TestOpenStartsOneTunnelForConcurrentOpens(t *testing.T) {
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
	port := freePort(t)
	k := NewMockKind(gomock.NewController(t))
	k.EXPECT().Command(gomock.Any()).Return(listener(port)).Times(1)
	k.EXPECT().Pids(gomock.Any()).Return([]int{1}, nil).AnyTimes()
	type got struct {
		result Result
		err    error
	}
	results := make(chan got, 2)
	for range 2 {
		go func() {
			r, err := open(k, "a", conn(port), testWait, testTick)
			results <- got{r, err}
		}()
	}
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	assert.ElementsMatch(t, []Result{Opened, Already}, []Result{first.result, second.result})
}

func TestCloseTunnel(t *testing.T) {
	t.Parallel()
	sleeper := exec.Command("sleep", "30")
	require.NoError(t, sleeper.Start())
	t.Cleanup(func() { _ = sleeper.Process.Kill() })
	held := freePort(t)
	argv, env := listener(held)
	tunnel := exec.Command(argv[0])
	tunnel.Env = env
	require.NoError(t, tunnel.Start())
	t.Cleanup(func() { _ = tunnel.Process.Kill() })
	require.Eventually(t, func() bool { up, _ := IsOpen(conn(held)); return up }, testWait, testTick)
	listening := listen(t)
	tcs := []struct {
		name string
		port int
		init func(k *MockKind)
		want Result
	}{
		{"kills our tunnel", freePort(t), func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{sleeper.Process.Pid}, nil)
		}, Closed},
		{"waits for our tunnel to free the port", held, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{tunnel.Process.Pid}, nil)
		}, Closed},
		{"keeps a foreign tunnel", listening, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, Foreign},
		{"direct connection has no tunnel", listening, nil, None},
		{"nothing open", freePort(t), func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, None},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var k Kind
			if tc.init != nil {
				m := NewMockKind(gomock.NewController(t))
				tc.init(m)
				k = m
			}
			got, err := closeTunnel(k, conn(tc.port), testWait, testTick)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			up, _ := IsOpen(conn(tc.port))
			assert.Equal(t, tc.port == listening, up, "port open after close")
		})
	}
	assert.Error(t, sleeper.Wait(), "terminated by SIGTERM")
	assert.Error(t, tunnel.Wait(), "terminated by SIGTERM")
}

func TestNew(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Tunnels: map[string]config.Tool{"tsh": {Bin: "/opt/tsh"}}}
	type want struct {
		kind bool
		err  string
	}
	tcs := []struct {
		name string
		conn config.Conn
		want want
	}{
		{"no tunnel", config.Conn{}, want{}},
		{"tsh", conn(1), want{kind: true}},
		{"unknown kind", config.Conn{Tunnel: &config.Tunnel{Type: "ssh"}}, want{err: "unsupported tunnel"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, err := New(cfg, tc.conn)
			assert.Equal(t, tc.want.kind, k != nil)
			assert.Equal(t, tc.want.err == "", err == nil, "error: %v", err)
			assert.Contains(t, errText(err), tc.want.err)
		})
	}
}
