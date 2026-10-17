package proc_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/proc"
)

// How Spawn ended in one comparable word
func outcome(err error) string {
	var exit *proc.ExitError
	switch {
	case err == nil:
		return "ready"
	case errors.As(err, &exit):
		return fmt.Sprintf("exit %d", exit.Code)
	case errors.Is(err, proc.ErrTimeout):
		return "timeout"
	}
	return err.Error()
}

func TestSpawn(t *testing.T) {
	t.Parallel()
	const wait = 500 * time.Millisecond
	type want struct {
		outcome string
		log     string
		fast    bool
	}
	tcs := []struct {
		name  string
		argv  []string
		ready bool
		want  want
	}{
		{"ready returns without waiting", []string{"sleep", "5"}, true, want{outcome: "ready", fast: true}},
		{"early exit fails fast with its code", []string{"sh", "-c", "echo 'not logged in' >&2; exit 3"}, false,
			want{outcome: "exit 3", log: "not logged in\n", fast: true}},
		{"timeout terminates the child", []string{"sleep", "30"}, false, want{outcome: "timeout"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := filepath.Join(t.TempDir(), "logs", "a.log")
			start := time.Now()
			err := proc.Spawn(tc.argv, os.Environ(), log, func() bool { return tc.ready }, wait, 20*time.Millisecond)
			assert.Equal(t, tc.want.outcome, outcome(err))
			assert.Equal(t, tc.want.fast, time.Since(start) < wait, "returned before the wait ran out")
			assert.Equal(t, tc.want.log+"log: "+log, proc.LogTail(log))
			fi, err := os.Stat(log)
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
		})
	}
}

// A ready state that shows up after the child exited isn't the child's
func TestSpawnRefusesReadyAfterExit(t *testing.T) {
	t.Parallel()
	ready := func() bool {
		time.Sleep(200 * time.Millisecond)
		return true
	}
	err := proc.Spawn([]string{"sh", "-c", "exit 3"}, os.Environ(), filepath.Join(t.TempDir(), "a.log"),
		ready, time.Second, 20*time.Millisecond)
	assert.Equal(t, "exit 3", outcome(err))
}

func TestSpawnWithoutCommand(t *testing.T) {
	t.Parallel()
	err := proc.Spawn(
		nil,
		nil,
		filepath.Join(t.TempDir(), "a.log"),
		func() bool { return true },
		time.Second,
		time.Millisecond,
	)
	assert.EqualError(t, err, "no command to run")
}

// The second lock waits until the first is released
func TestLock(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "logs", "a.lock")
	unlock, err := proc.Lock(path)
	require.NoError(t, err)
	acquired := make(chan struct{})
	go func() {
		second, err := proc.Lock(path)
		if err == nil {
			second()
		}
		close(acquired)
	}()
	gotLock := func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}
	assert.Never(t, gotLock, 200*time.Millisecond, 20*time.Millisecond)
	unlock()
	assert.Eventually(t, gotLock, 2*time.Second, 20*time.Millisecond)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

func TestLogTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	long := filepath.Join(dir, "long.log")
	require.NoError(t, os.WriteFile(long, []byte("1\n2\n3\n4\n5\n6\n7\n"), 0o600))
	short := filepath.Join(dir, "short.log")
	require.NoError(t, os.WriteFile(short, []byte("1\n2\n"), 0o600))
	empty := filepath.Join(dir, "empty.log")
	require.NoError(t, os.WriteFile(empty, []byte("\n"), 0o600))
	missing := filepath.Join(dir, "missing.log")
	tcs := []struct {
		name string
		path string
		want string
	}{
		{"last 5 lines", long, "3\n4\n5\n6\n7\nlog: " + long},
		{"fewer lines than 5", short, "1\n2\nlog: " + short},
		{"empty log gives the path alone", empty, "log: " + empty},
		{"missing log gives the path alone", missing, "log: " + missing},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, proc.LogTail(tc.path))
		})
	}
}
