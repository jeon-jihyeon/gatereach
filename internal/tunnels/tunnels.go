// Package tunnels opens tunnels on demand and keeps them up until closed
package tunnels

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=kind_mock_test.go -package=tunnels -self_package=github.com/jeon-jihyeon/gatereach/internal/tunnels -mock_names=Kind=MockKind github.com/jeon-jihyeon/gatereach/internal/tunnels Kind

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
)

// Tunnels keep no state file
// The process list is the state
type Kind interface {
	// Command line and environment of the tunnel process
	Command(conn config.Conn) (argv, env []string)
	// Processes owned by the current user whose command line matches exactly
	Pids(conn config.Conn) ([]int, error)
	Discover() ([]Target, error)
}

// A target the tunnel can reach
type Target struct {
	// The name the tunnel command takes
	Target string
	// The name people know it by
	Name string
	// The gatereach client name
	// Empty for an unknown protocol
	Client string
	Users  []string
}

// Constructor per kind name
// Each takes the `[tunnels.<kind>]` values of the config file
var Kinds = map[string]func(config.Tool) Kind{"tsh": newTsh}

func Names() []string {
	return slices.Sorted(maps.Keys(Kinds))
}

// Outcome of opening or closing a tunnel
type Result string

const (
	Already Result = "already" // our tunnel or a direct connection already answers
	Foreign Result = "foreign" // another process holds the port so the target can't be confirmed
	Opened  Result = "opened"
	Closed  Result = "closed"
	None    Result = "none" // nothing was open
)

const (
	openWait = 20 * time.Second
	openTick = 300 * time.Millisecond
	dialWait = 300 * time.Millisecond
	// How long close waits for SIGTERM to end the tunnel
	closeWait = 5 * time.Second
)

// Swapped in tests to act as another user
var checkOwner = proc.CheckOwner

// nil when the connection has no tunnel
func New(cfg *config.Config, conn config.Conn) (Kind, error) {
	if conn.Tunnel == nil {
		return nil, nil
	}
	return Of(cfg, conn.Tunnel.Type)
}

// The kind name is also the binary looked up on PATH when the config sets none
func Of(cfg *config.Config, typ string) (Kind, error) {
	if err := Check(typ); err != nil {
		return nil, err
	}
	return Kinds[typ](cfg.Tool(typ, typ)), nil
}

func Check(typ string) error {
	if _, ok := Kinds[typ]; !ok {
		return fmt.Errorf("unsupported tunnel: %s\navailable: %s", typ, strings.Join(Names(), " "))
	}
	return nil
}

// ok is false for a connection without a port
// Errors such as a failed name lookup count as closed too
func IsOpen(conn config.Conn) (open, ok bool) {
	if conn.Port == 0 {
		return false, false
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort(conn.Host, strconv.Itoa(conn.Port)), dialWait)
	if err != nil {
		return false, true
	}
	_ = c.Close()
	return true, true
}

func Open(cfg *config.Config, alias string, conn config.Conn) (Result, error) {
	k, err := New(cfg, conn)
	if err != nil {
		return "", err
	}
	return open(k, alias, conn, openWait, openTick)
}

// Starts the tunnel only under the lock
// since a second tunnel for the same alias would fail to bind the port and exit
func open(k Kind, alias string, conn config.Conn, wait, tick time.Duration) (Result, error) {
	if up, _ := IsOpen(conn); up {
		return holder(k, alias, conn)
	}
	if k == nil {
		return "", fmt.Errorf("%s is closed with no tunnel configured: %s:%d", alias, conn.Host, conn.Port)
	}
	unlock, err := proc.Lock(config.LogPath(alias, ".lock"))
	if err != nil {
		return "", err
	}
	defer unlock()
	// Another gatereach may have opened it while this one waited for the lock
	if up, _ := IsOpen(conn); up {
		return holder(k, alias, conn)
	}
	argv, env := k.Command(conn)
	path := config.LogPath(alias, ".log")
	ready := func() bool { up, _ := IsOpen(conn); return up }
	err = proc.Spawn(argv, env, path, ready, wait, tick)
	var exit *proc.ExitError
	switch {
	case err == nil:
		if err := checkOwner(alias, conn.Port); err != nil {
			return "", err
		}
		return Opened, nil
	case errors.As(err, &exit):
		return "", fmt.Errorf("%s open failed: tunnel %s\n%s", alias, exit, proc.LogTail(path))
	case errors.Is(err, proc.ErrTimeout):
		return "", fmt.Errorf("%s open failed: port didn't open within %s\ncheck the tunnel login\n%s", alias, wait, proc.LogTail(path))
	}
	return "", fmt.Errorf("%s tunnel couldn't run: %w", alias, err)
}

// Who holds the open port
// 1. Without a tunnel it's a direct connection the user chose and a local database often runs as its own system user
// 2. With a tunnel the port must belong to the current user before anything reads it as ours or foreign
func holder(k Kind, alias string, conn config.Conn) (Result, error) {
	if k == nil {
		return Already, nil
	}
	if err := checkOwner(alias, conn.Port); err != nil {
		return "", err
	}
	pids, err := k.Pids(conn)
	if err != nil {
		return "", err
	}
	if len(pids) == 0 {
		return Foreign, nil
	}
	return Already, nil
}

func Close(cfg *config.Config, conn config.Conn) (Result, error) {
	k, err := New(cfg, conn)
	if err != nil {
		return "", err
	}
	return closeTunnel(k, conn, closeWait, openTick)
}

// A direct connection has no tunnel to close
// Waits for the port to close or the tunnel to end
// so an open right after it never finds the dying tunnel and reports already
func closeTunnel(k Kind, conn config.Conn, wait, tick time.Duration) (Result, error) {
	if k == nil {
		return None, nil
	}
	pids, err := k.Pids(conn)
	if err != nil {
		return "", err
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return "", err
		}
	}
	if len(pids) > 0 {
		for end := time.Now().Add(wait); time.Now().Before(end); time.Sleep(tick) {
			if up, _ := IsOpen(conn); !up || !anyAlive(pids) {
				break
			}
		}
		return Closed, nil
	}
	if up, _ := IsOpen(conn); up {
		return Foreign, nil
	}
	return None, nil
}

// Signal 0 checks a process without touching it
func anyAlive(pids []int) bool {
	return slices.ContainsFunc(pids, func(pid int) bool { return !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
}
