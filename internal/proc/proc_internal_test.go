package proc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A process that ignores SIGTERM ends by SIGKILL after the grace period so a timeout never hangs
// Each script gets a pid file as $1 and runs a sleep that writes its pid there and ignores SIGTERM
func TestSpawnKillsAGroupThatIgnoresSIGTERM(t *testing.T) {
	t.Parallel()
	const sleeper = `sh -c 'trap "" TERM; echo $$ > "$1"; exec sleep 30' sh "$1"`
	tcs := []struct {
		name   string
		script string
	}{
		{"child ignores SIGTERM", `trap "" TERM; ` + sleeper},
		{"grandchild ignores SIGTERM after the child exits", sleeper + ` & wait`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "pid")
			start := time.Now()
			err := spawn([]string{"sh", "-c", tc.script, "sh", pidFile}, os.Environ(), filepath.Join(dir, "logs", "a.log"),
				func() bool { return false }, 500*time.Millisecond, 20*time.Millisecond, 200*time.Millisecond)
			assert.ErrorIs(t, err, ErrTimeout)
			assert.Less(t, time.Since(start), 5*time.Second)
			b, err := os.ReadFile(pidFile)
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			require.NoError(t, err)
			assert.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) },
				2*time.Second, 20*time.Millisecond, "sleep %d survived", pid)
		})
	}
}

// Not parallel since it swaps listenerUIDs
func TestCheckOwner(t *testing.T) {
	me := os.Getuid()
	held := "a port 21000 is held by another user\nclose that listener or change the port"
	tcs := []struct {
		name string
		uids []int
		err  error
		want string
	}{
		{"only the current user listens", []int{me, me}, nil, ""},
		{"another user listens", []int{me + 1}, nil, held},
		{"another user listens beside us", []int{me, me + 1}, nil, held},
		{"no visible listener", nil, nil, held},
		{"lookup fails", nil, errors.New("lsof couldn't run"), "a port 21000 owner couldn't be checked: lsof couldn't run"},
	}
	t.Cleanup(func() { listenerUIDs = listeners })
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			listenerUIDs = func(int) ([]int, error) { return tc.uids, tc.err }
			assert.Equal(t, tc.want, errText(CheckOwner("a", 21000)))
		})
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The real lookup finds a listener of this test process
func TestListeners(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	uids, err := listeners(ln.Addr().(*net.TCPAddr).Port)
	require.NoError(t, err)
	assert.Contains(t, uids, os.Getuid())
}

func TestParseLsof(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		out  string
		want []int
	}{
		{"one process", "p11\nu501\nf5\n", []int{501}},
		{"two processes", "p11\nu501\nf5\np12\nu0\nf7\n", []int{501, 0}},
		{"nothing matched", "", nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, parseLsof([]byte(tc.out)))
		})
	}
}

func TestParseProcNet(t *testing.T) {
	t.Parallel()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	// 0x5208 is 21000 and 0x5209 is 21001
	tcs := []struct {
		name  string
		table string
		want  []int
	}{
		{"listener on the port", header +
			"   0: 0100007F:5208 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 1 1 0\n",
			[]int{1000}},
		{"tcp6 listener", header +
			"   0: 00000000000000000000000001000000:5208 00000000000000000000000000000000:0000 0A " +
			"00000000:00000000 00:00000000 00000000     0        0 1 1 0\n", []int{0}},
		{"established connection on the port is skipped", header +
			"   0: 0100007F:5208 0100007F:D431 01 00000000:00000000 00:00000000 00000000  1000        0 1 1 0\n", nil},
		{"another port is skipped", header +
			"   0: 0100007F:5209 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 1 1 0\n", nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, parseProcNet([]byte(tc.table), 21000))
		})
	}
}
