package tunnels

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
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

func reservePort(t *testing.T) (int, func()) {
	t.Helper()
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	require.NoError(t, err)
	release := sync.OnceFunc(func() { assert.NoError(t, syscall.Close(fd)) })
	t.Cleanup(release)
	require.NoError(t, syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	addr, err := syscall.Getsockname(fd)
	require.NoError(t, err)
	return addr.(*syscall.SockaddrInet4).Port, release
}

const (
	listenEnv = "GATEREACH_TEST_LISTEN"
	stopEnv   = "GATEREACH_TEST_STOP"
)

func TestMain(m *testing.M) {
	if port := os.Getenv(listenEnv); port != "" {
		stop, err := net.Listen("unix", os.Getenv(stopEnv))
		if err != nil {
			os.Exit(3)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			_ = stop.Close()
			os.Exit(3)
		}
		c, err := stop.Accept()
		if c != nil {
			_ = c.Close()
		}
		_ = ln.Close()
		_ = stop.Close()
		if err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func listener(t *testing.T, port int) ([]string, []string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gatereach-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "stop.sock")
	stop := sync.OnceFunc(func() {
		c, err := net.DialTimeout("unix", path, testWait)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return
		}
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.Eventually(t, func() bool { up, _ := IsOpen(conn(port)); return !up }, testWait, testTick)
	})
	t.Cleanup(stop)
	return []string{os.Args[0]}, append(os.Environ(), listenEnv+"="+strconv.Itoa(port), stopEnv+"="+path), stop
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
		init  func(t *testing.T, k *MockKind, port int, release func())
		owner error
		want  want
	}{
		{"our tunnel holds the port", listening, func(_ *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{1}, nil)
		}, nil, want{result: Already}},
		{"another program holds the port", listening, func(_ *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, nil, want{result: Foreign}},
		{"another user holds the port", listening, func(*testing.T, *MockKind, int, func()) {},
			errOtherUser, want{err: errOtherUser.Error()}},
		{"process list fails", listening, func(_ *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, errors.New("ps failed"))
		}, nil, want{err: "ps failed"}},
		{"direct connection skips the owner check", listening, nil, errOtherUser, want{result: Already}},
		{"closed port without a tunnel", 0, nil, nil, want{err: "a is closed with no tunnel configured"}},
		{"starts and waits for the port", 0, func(t *testing.T, k *MockKind, port int, release func()) {
			argv, env, _ := listener(t, port)
			k.EXPECT().Command(gomock.Any()).DoAndReturn(func(config.Conn) ([]string, []string) {
				release()
				return argv, env
			})
		}, nil, want{result: Opened}},
		{"another user holds the port the tunnel opened", 0,
			func(t *testing.T, k *MockKind, port int, release func()) {
				argv, env, _ := listener(t, port)
				k.EXPECT().Command(gomock.Any()).DoAndReturn(func(config.Conn) ([]string, []string) {
					release()
					return argv, env
				})
			}, errOtherUser, want{err: errOtherUser.Error()}},
		{"early exit shows the log", 0, func(_ *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().
				Command(gomock.Any()).
				Return([]string{"sh", "-c", "echo 'ERROR: not logged in' >&2; exit 3"}, os.Environ())
		}, nil, want{err: "a open failed: tunnel exit code 3\nERROR: not logged in"}},
		{"timeout asks to check the login", 0, func(_ *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().Command(gomock.Any()).Return([]string{"sleep", "30"}, os.Environ())
		}, nil, want{err: "check the tunnel login"}},
		{"missing tunnel program", 0, func(t *testing.T, k *MockKind, _ int, _ func()) {
			k.EXPECT().Command(gomock.Any()).Return([]string{filepath.Join(t.TempDir(), "no-tsh")}, os.Environ())
		}, nil, want{err: "a tunnel couldn't run"}},
	}
	t.Cleanup(func() { checkOwner = proc.CheckOwner })
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			port, release := tc.port, func() {}
			if port == 0 {
				port, release = reservePort(t)
			}
			checkOwner = proc.CheckOwner
			if tc.owner != nil {
				checkOwner = func(string, int) error { return tc.owner }
			}
			var k Kind
			if tc.init != nil {
				m := NewMockKind(gomock.NewController(t))
				tc.init(t, m, port, release)
				k = m
			}
			got, err := open(k, "a", conn(port), testWait, testTick)
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
	port, release := reservePort(t)
	argv, env, _ := listener(t, port)
	k := NewMockKind(gomock.NewController(t))
	k.EXPECT().Command(gomock.Any()).DoAndReturn(func(config.Conn) ([]string, []string) {
		release()
		return argv, env
	}).Times(1)
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

// Concurrent forks can retain a reserved socket until the child execs
func TestCloseTunnel(t *testing.T) {
	sleeper := exec.Command("sleep", "30")
	require.NoError(t, sleeper.Start())
	t.Cleanup(func() { _ = sleeper.Process.Kill() })
	held, release := reservePort(t)
	argv, env, stop := listener(t, held)
	tunnel := exec.Command(argv[0])
	tunnel.Env = env
	release()
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
		{"kills our tunnel", 0, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{sleeper.Process.Pid}, nil)
		}, Closed},
		{"waits for our tunnel to free the port", held, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{tunnel.Process.Pid}, nil)
		}, Closed},
		{"keeps a foreign tunnel", listening, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, Foreign},
		{"direct connection has no tunnel", listening, nil, None},
		{"nothing open", 0, func(k *MockKind) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, None},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			port := tc.port
			if port == 0 {
				port, _ = reservePort(t)
			}
			var k Kind
			if tc.init != nil {
				m := NewMockKind(gomock.NewController(t))
				tc.init(m)
				k = m
			}
			got, err := closeTunnel(k, conn(port), testWait, testTick)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			up, _ := IsOpen(conn(port))
			assert.Equal(t, port == listening, up, "port open after close")
		})
	}
	assert.Error(t, sleeper.Wait(), "terminated by SIGTERM")
	stop()
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
