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
)

const (
	testWait = time.Second
	testTick = 20 * time.Millisecond
)

// 열린 포트 하나
func listen(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

// 닫힌 포트 하나
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return p
}

// 테스트 바이너리를 터널 대신 띄우는 도우미 모드
// 값의 포트를 2초 동안 열어 둠
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

// 포트를 여는 도우미 자식의 명령줄과 환경
func listener(port int) ([]string, []string) {
	return []string{os.Args[0]}, append(os.Environ(), listenEnv+"="+strconv.Itoa(port))
}

func conn(port int) config.Conn {
	return config.Conn{Client: "mysql", Host: "127.0.0.1", Port: port, User: "u", Tunnel: &config.Tunnel{Type: "tsh", Target: "t"}}
}

// GATEREACH_CONFIG를 바꾸므로 병렬 실행하지 않음
func TestOpen(t *testing.T) {
	t.Setenv("GATEREACH_CONFIG", filepath.Join(t.TempDir(), "conns.toml"))
	listening := listen(t)
	type want struct {
		result Result
		err    string
	}
	tcs := []struct {
		name   string
		port   int
		noKind bool
		init   func(k *MockKind, port int)
		want   want
	}{
		{"our tunnel holds the port", listening, false, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return([]int{1}, nil)
		}, want{result: Already}},
		{"another program holds the port", listening, false, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, nil)
		}, want{result: Foreign}},
		{"process list fails", listening, false, func(k *MockKind, _ int) {
			k.EXPECT().Pids(gomock.Any()).Return(nil, errors.New("ps 실패"))
		}, want{err: "ps 실패"}},
		{"direct connection", listening, true, nil, want{result: Already}},
		{"closed port without a tunnel", freePort(t), true, nil, want{err: "닫혀 있고 터널 설정 없음"}},
		{"starts and waits for the port", freePort(t), false, func(k *MockKind, port int) {
			k.EXPECT().Command(gomock.Any()).Return(listener(port))
		}, want{result: Opened}},
		{"early exit shows the log", freePort(t), false, func(k *MockKind, _ int) {
			k.EXPECT().Command(gomock.Any()).Return([]string{"sh", "-c", "echo 'ERROR: not logged in' >&2; exit 3"}, os.Environ())
		}, want{err: "ERROR: not logged in"}},
		{"timeout asks to check the login", freePort(t), false, func(k *MockKind, _ int) {
			k.EXPECT().Command(gomock.Any()).Return([]string{"sleep", "30"}, os.Environ())
		}, want{err: "터널 로그인 상태 확인"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var k Kind
			if !tc.noKind {
				m := NewMockKind(gomock.NewController(t))
				if tc.init != nil {
					tc.init(m, tc.port)
				}
				k = m
			}
			got, err := open(k, "a", conn(tc.port), testWait, testTick)
			if tc.want.err != "" {
				assert.ErrorContains(t, err, tc.want.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.result, got)
		})
	}
}

// 닫힌 별칭을 동시에 열어도 터널은 하나만 띄우고 나머지는 이미 열림
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
	sleeper := exec.Command("sleep", "30")
	require.NoError(t, sleeper.Start())
	t.Cleanup(func() { _ = sleeper.Process.Kill() })
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
			got, err := closeTunnel(k, conn(tc.port))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
	assert.Error(t, sleeper.Wait(), "terminated by SIGTERM")
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
		{"unknown kind", config.Conn{Tunnel: &config.Tunnel{Type: "ssh"}}, want{err: "지원하지 않는 터널"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, err := New(cfg, tc.conn)
			if tc.want.err != "" {
				assert.ErrorContains(t, err, tc.want.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.kind, k != nil)
		})
	}
}
