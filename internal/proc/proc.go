// Package proc starts the child processes tunnels and sessions need with their locks and port owner checks
package proc

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// How long a timed-out child's group gets between SIGTERM and SIGKILL
const killWait = 5 * time.Second

// Log lines LogTail shows
const tailLines = 5

// The child exited before it was ready
type ExitError struct {
	Code int
}

func (e *ExitError) Error() string { return fmt.Sprintf("exit code %d", e.Code) }

// The child wasn't ready in time and was stopped
var ErrTimeout = errors.New("timed out")

// 1. The child runs in a new session so Ctrl-C in the parent or its shell never reaches it
// 2. stdout and stderr go to a log file emptied on every start so it never grows past one run
// 3. Returns without waiting on the child once ready is true while the child still runs
// 4. On timeout the child's process group gets SIGTERM
// 5. A group still alive after the grace period gets SIGKILL so grandchildren end even when the child already exited
func Spawn(argv, env []string, logPath string, ready func() bool, wait, tick time.Duration) error {
	return spawn(argv, env, logPath, ready, wait, tick, killWait)
}

func spawn(argv, env []string, logPath string, ready func() bool, wait, tick, grace time.Duration) error {
	if len(argv) == 0 {
		return errors.New("no command to run")
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// The child holds its own copy of the file
	defer out.Close()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if ready() {
			// A child that already exited didn't make the ready state
			// so a port another process opened meanwhile never counts as its own
			select {
			case <-exited:
				return &ExitError{Code: cmd.ProcessState.ExitCode()}
			default:
				return nil
			}
		}
		select {
		case <-exited:
			return &ExitError{Code: cmd.ProcessState.ExitCode()}
		case <-time.After(tick):
		}
	}
	// Setsid makes the child the group leader so the negative pid is the whole group
	group := -cmd.Process.Pid
	_ = syscall.Kill(group, syscall.SIGTERM)
	// The child may exit while a grandchild that ignores SIGTERM stays
	// so the grace period watches the whole group and not only the child
	for end := time.Now().Add(grace); time.Now().Before(end); time.Sleep(tick) {
		if !groupAlive(group) {
			<-exited
			return ErrTimeout
		}
	}
	_ = syscall.Kill(group, syscall.SIGKILL)
	<-exited
	return ErrTimeout
}

// Signal 0 checks the group without touching it
// ESRCH means no member is left
func groupAlive(group int) bool {
	return !errors.Is(syscall.Kill(group, 0), syscall.ESRCH)
}

// An exclusive lock that waits for any other process holding the same path
// The kernel releases it when the process ends so it never leaves stale state
func Lock(path string) (unlock func(), err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Closing the file releases the flock
	return func() { _ = f.Close() }, nil
}

// The last log lines that show why a child failed and then the log path
// An empty or unreadable log gives the path alone
func LogTail(path string) string {
	b, err := os.ReadFile(path)
	tail := strings.TrimSpace(string(b))
	if err != nil || tail == "" {
		return "log: " + path
	}
	lines := strings.Split(tail, "\n")
	if len(lines) > tailLines {
		lines = lines[len(lines)-tailLines:]
	}
	return strings.Join(lines, "\n") + "\nlog: " + path
}
