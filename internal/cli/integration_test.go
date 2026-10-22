//go:build integration

package cli_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
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

	"github.com/jeon-jihyeon/gatereach/internal/cli"
	"github.com/jeon-jihyeon/gatereach/internal/session"
)

// Runs the real binary against a local redis-server from session start to close
// Skips when redis-server isn't installed
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
	require.NoError(
		t,
		os.WriteFile(cfg, []byte("[conns.r]\nclient = \"redis\"\nport = "+strconv.Itoa(port)+"\nwrite = true\n"), 0o600),
	)
	gr := func(args ...string) (int, string, string) {
		cmd := exec.Command(bin, args...)
		var out, errw strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &errw
		err := cmd.Run()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		}
		return code, out.String(), errw.String()
	}
	t.Cleanup(func() { gr("close", "r") })

	// The next start clears the file a dead session left behind
	require.NoError(t, os.MkdirAll(session.Dir(), 0o700))
	require.NoError(t, os.WriteFile(session.SocketPath("r"), nil, 0o600))

	code, out, errw := gr("q", "r", "SET k v", "--write")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, errw, "r session started")
	assert.Equal(t, "{\"value\":\"OK\"}\n", out)

	code, out, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.NotContains(t, errw, "session started", "the session is reused")
	assert.Equal(t, "{\"value\":\"v\"}\n", out)

	code, _, errw = gr("q", "r", "SET large 9223372036854775805", "--write")
	require.Equal(t, 0, code, errw)
	code, out, errw = gr("q", "r", "INCR large", "--write", "--events")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, out, `"record":{"value":9223372036854775806}`)
	code, out, errw = gr("q", "r", "GET absent", "--events")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, out, `"record":{"value":null}`)

	code, _, errw = gr("q", "r", "SET payload "+strings.Repeat("x", 100<<10), "--write")
	require.Equal(t, 0, code, errw)
	closed, broken := io.Pipe()
	require.NoError(t, closed.Close())
	var failure strings.Builder
	assert.Equal(t, 1, cli.Run([]string{"q", "r", "GET payload", "--events"}, broken, &failure))
	assert.Equal(t, 1, strings.Count(failure.String(), io.ErrClosedPipe.Error()))
	require.NoError(t, broken.Close())

	// The next q replaces a session an older binary started with its own binary
	old := filepath.Join(dir, "gatereach-old")
	build = exec.Command(
		"go",
		"build",
		"-o",
		old,
		"-ldflags",
		"-X github.com/jeon-jihyeon/gatereach/internal/version.Version=0.0.1",
		"../../cmd/gatereach",
	)
	build.Stderr = os.Stderr
	require.NoError(t, build.Run())
	require.NoError(t, exec.Command(old, "close", "r").Run())
	require.NoError(t, exec.Command(old, "open", "r").Run())
	code, out, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.Contains(t, errw, "r session restarted on the new version")
	assert.Equal(t, "{\"value\":\"v\"}\n", out)
	code, _, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)
	assert.NotContains(t, errw, "restarted", "the new session is reused")

	// A read under --max reports what it used last
	code, _, errw = gr("q", "r", "GET k", "--max", "5")
	require.Equal(t, 0, code, errw)
	assert.True(t, strings.HasSuffix(errw, "r read 1 reply element of --max 5 reply elements\n"), errw)

	// A follow that reaches --max ends normally after the paid record
	code, out, errw = gr("q", "r", "GET k", "--follow", "--interval", "1", "--max", "2")
	require.Equal(t, 0, code, errw)
	assert.Equal(t, "{\"value\":\"v\"}\n", out)
	assert.True(t, strings.HasSuffix(errw, "r stopped at --max: read 2 reply elements of 2 reply elements\n"), errw)

	// A package keeps its cap and says when the cap stopped the run
	for _, tc := range []struct {
		name    string
		max     int
		stopped any
	}{
		{"under the cap", 5, nil},
		{"stopped at the cap", 1, "budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg := filepath.Join(t.TempDir(), "out")
			code, _, errw := gr("q", "r", "GET k", "--max", strconv.Itoa(tc.max), "--out", pkg)
			require.Equal(t, 0, code, errw)
			b, err := os.ReadFile(filepath.Join(pkg, "datapackage.json"))
			require.NoError(t, err)
			var d struct {
				Gatereach map[string]any `json:"gatereach"`
			}
			require.NoError(t, json.Unmarshal(b, &d))
			assert.Equal(t, true, d.Gatereach["complete"])
			assert.Equal(t, tc.stopped, d.Gatereach["stopped"])
			assert.Equal(t, float64(tc.max), d.Gatereach["max"])
			assert.Equal(t, "reply elements", d.Gatereach["unit"])
		})
	}

	type outcome struct {
		Status  string `json:"status"`
		Session struct {
			Rows  int  `json:"rows"`
			Spent bool `json:"spent"`
		} `json:"session"`
	}
	for _, tc := range []struct {
		name   string
		args   []string
		status string
		spent  bool
	}{
		{"finished", []string{"q", "r", "GET k", "--events"}, "finished", false},
		{"budget", []string{"q", "r", "GET k", "--events", "--max", "1"}, "budget", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errw := gr(tc.args...)
			require.Equal(t, 0, code, errw)
			lines := strings.Split(strings.TrimSpace(out), "\n")
			require.Len(t, lines, 2)
			assert.JSONEq(t, `{"schema":"gatereach/v1","type":"record","record":{"value":"v"}}`, lines[0])
			var end outcome
			require.NoError(t, json.Unmarshal([]byte(lines[1]), &end))
			assert.Equal(t, tc.status, end.Status)
			assert.Equal(t, tc.spent, end.Session.Spent)
			assert.Equal(t, 1, end.Session.Rows)
		})
	}

	// A blocking command past --timeout fails with the timeout and the session keeps serving
	code, out, errw = gr("q", "r", "BLPOP missing 10", "--write", "--timeout", "0.2", "--events")
	assert.Equal(t, 1, code)
	assert.Contains(t, errw, "query ran past --timeout 200ms")
	assert.Contains(t, out, `"status":"error"`)
	assert.Contains(t, out, `"session":`)
	code, _, errw = gr("q", "r", "GET k")
	require.Equal(t, 0, code, errw)

	_, out, _ = gr("ls")
	assert.Equal(t, "live", strings.Fields(out)[6])
	_, out, _ = gr("open", "r")
	assert.Contains(t, out, "r session already running")

	// An interrupted follow exits cleanly
	follow := exec.Command(bin, "q", "r", "GET k", "--follow", "--interval", "1", "--events")
	stdout, err := follow.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, follow.Start())
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.JSONEq(t, `{"schema":"gatereach/v1","type":"record","record":{"value":"v"}}`, line)
	require.NoError(t, follow.Process.Signal(syscall.SIGINT))
	line, err = reader.ReadString(10)
	require.NoError(t, err)
	assert.Contains(t, line, `"status":"stopped"`)
	assert.NotContains(t, line, `"session":`)
	assert.NoError(t, follow.Wait())

	// An interrupted single read exits 130
	block := exec.Command(bin, "q", "r", "BLPOP missing 10", "--write", "--events")
	var blockOutput strings.Builder
	block.Stdout = &blockOutput
	require.NoError(t, block.Start())
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, block.Process.Signal(syscall.SIGINT))
	err = block.Wait()
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, 130, ee.ExitCode())
	assert.Contains(t, blockOutput.String(), `"status":"interrupted"`)
	assert.NotContains(t, blockOutput.String(), `"session":`)

	code, out, _ = gr("close", "r")
	assert.Equal(t, 0, code)
	assert.Equal(t, "r closed\n", out)
	assert.False(t, session.Alive("r"))
	code, out, _ = gr("close", "r")
	assert.Equal(t, 0, code)
	assert.Equal(t, "r already closed\n", out)
}
