//go:build integration

package cli_test

import (
	"bufio"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/session"
)

// 실제 바이너리와 로컬 redis-server로 세션 시작부터 종료까지
// redis-server가 없으면 건너뜀
func TestSessionLifecycleAgainstRedis(t *testing.T) {
	server, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "gatereach")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/gatereach")
	build.Stderr = os.Stderr
	require.NoError(t, build.Run())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	redis := exec.Command(server, "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
	require.NoError(t, redis.Start())
	t.Cleanup(func() { _ = redis.Process.Kill(); _ = redis.Wait() })
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 5*time.Second, 50*time.Millisecond)

	cfg := filepath.Join(dir, "conns.toml")
	t.Setenv("GATEREACH_CONFIG", cfg)
	t.Cleanup(func() { _ = os.RemoveAll(session.Dir()) })
	require.NoError(t, os.WriteFile(cfg, []byte("[conns.r]\nclient = \"redis\"\nport = "+strconv.Itoa(port)+"\n"), 0o600))
	gr := func(args ...string) (int, string, string) {
		cmd := exec.Command(bin, args...)
		var out, errw strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &errw
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return code, out.String(), errw.String()
	}
	t.Cleanup(func() { gr("close", "r") })

	// 죽은 세션이 남긴 파일은 다음 시작이 치움
	require.NoError(t, os.MkdirAll(session.Dir(), 0o700))
	require.NoError(t, os.WriteFile(session.SocketPath("r"), nil, 0o600))

	code, out, errw := gr("q", "r", "SET k v", "--write")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, errw, "r 세션 시작")
	assert.Equal(t, "{\"value\":\"OK\"}\n", out)

	code, out, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.NotContains(t, errw, "세션 시작", "the session is reused")
	assert.Equal(t, "{\"value\":\"v\"}\n", out)

	// 이전 버전 바이너리가 띄운 세션은 다음 q가 자기 바이너리로 바꿈
	old := filepath.Join(dir, "gatereach-old")
	build = exec.Command("go", "build", "-o", old, "-ldflags", "-X github.com/jeon-jihyeon/gatereach/internal/version.Version=0.0.1", "../../cmd/gatereach")
	build.Stderr = os.Stderr
	require.NoError(t, build.Run())
	require.NoError(t, exec.Command(old, "close", "r").Run())
	require.NoError(t, exec.Command(old, "open", "r").Run())
	code, out, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, errw, "r 세션을 새 버전으로 다시 시작")
	assert.Equal(t, "{\"value\":\"v\"}\n", out)
	code, _, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.NotContains(t, errw, "다시 시작", "the new session is reused")

	_, out, _ = gr("ls")
	assert.Equal(t, "live", strings.Fields(out)[6])
	_, out, _ = gr("open", "r")
	assert.Contains(t, out, "r 세션 이미 실행 중")

	// follow는 중단해도 정상 종료
	follow := exec.Command(bin, "q", "r", "GET k", "--follow", "--interval", "0.05")
	stdout, err := follow.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, follow.Start())
	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "{\"value\":\"v\"}\n", line)
	require.NoError(t, follow.Process.Signal(syscall.SIGINT))
	assert.NoError(t, follow.Wait())

	// 단건 조회를 중단하면 130
	block := exec.Command(bin, "q", "r", "BLPOP missing 10", "--write")
	require.NoError(t, block.Start())
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, block.Process.Signal(syscall.SIGINT))
	err = block.Wait()
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, 130, ee.ExitCode())

	code, out, _ = gr("close", "r")
	assert.Equal(t, 0, code)
	assert.Equal(t, "r 닫힘\n", out)
	assert.False(t, session.Alive("r"))
	code, out, _ = gr("close", "r")
	assert.Equal(t, 0, code)
	assert.Equal(t, "r 이미 닫힘\n", out)
}
