package proc_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/proc"
)

func TestSpawn(t *testing.T) {
	t.Parallel()
	type want struct {
		code    int
		timeout bool
		log     string
	}
	tcs := []struct {
		name  string
		argv  []string
		ready bool
		want  want
	}{
		{"ready returns without waiting", []string{"sleep", "5"}, true, want{}},
		{"early exit fails fast with its code", []string{"sh", "-c", "echo 'not logged in' >&2; exit 3"}, false, want{code: 3, log: "not logged in"}},
		{"timeout terminates the child", []string{"sleep", "30"}, false, want{timeout: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := filepath.Join(t.TempDir(), "logs", "a.log")
			start := time.Now()
			err := proc.Spawn(tc.argv, os.Environ(), log, func() bool { return tc.ready }, 500*time.Millisecond, 20*time.Millisecond)
			var exit *proc.ExitError
			switch {
			case tc.want.code != 0:
				require.True(t, errors.As(err, &exit), "%v", err)
				assert.Equal(t, tc.want.code, exit.Code)
				assert.Equal(t, tc.want.log, proc.Tail(log, 5))
				assert.Less(t, time.Since(start), 500*time.Millisecond)
			case tc.want.timeout:
				assert.ErrorIs(t, err, proc.ErrTimeout)
			default:
				assert.NoError(t, err)
			}
			fi, err := os.Stat(log)
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
		})
	}
}

// 두 번째 잠금은 첫 잠금이 풀릴 때까지 대기
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
	assert.Never(t, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 20*time.Millisecond)
	unlock()
	assert.Eventually(t, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}, 2*time.Second, 20*time.Millisecond)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

func TestTail(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "log")
	require.NoError(t, os.WriteFile(p, []byte("1\n2\n3\n"), 0o600))
	tcs := []struct {
		name string
		path string
		n    int
		want string
	}{
		{"last lines", p, 2, "2\n3"},
		{"fewer lines than asked", p, 9, "1\n2\n3"},
		{"missing file", p + ".missing", 2, ""},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, proc.Tail(tc.path, tc.n))
		})
	}
}
