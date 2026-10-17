package tunnels

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/jeon-jihyeon/gatereach/internal/config"
)

// Teleport protocol → gatereach client
var tshClients = map[string]string{"mysql": "mysql", "redis": "redis"}

type tsh struct {
	tool config.Tool
	// The current user's process list
	ps func() ([]byte, error)
}

func newTsh(tool config.Tool) Kind {
	return tsh{tool: tool, ps: listProcesses}
}

func listProcesses() ([]byte, error) {
	return exec.Command("ps", "-ww", "-U", strconv.Itoa(os.Getuid()), "-o", "pid=,command=").Output()
}

// TELEPORT_HOME points tsh at the profiles the Teleport Connect app keeps
func (t tsh) env() []string {
	if t.tool.Home == "" {
		return os.Environ()
	}
	return append(os.Environ(), "TELEPORT_HOME="+t.tool.Home)
}

// The = form and the -- before the target keep a value that starts with a dash from reading as a flag
// tsh parses with kingpin which takes everything after -- as arguments
func (t tsh) argv(conn config.Conn) []string {
	return []string{
		t.tool.Bin, "proxy", "db", "--tunnel", "--port", strconv.Itoa(conn.Port), "--db-user=" + conn.User,
		"--", conn.Tunnel.Target,
	}
}

func (t tsh) Command(conn config.Conn) ([]string, []string) {
	return t.argv(conn), t.env()
}

// A tunnel another program such as the Teleport Connect app opened has a different command line
// so it's left out
func (t tsh) Pids(conn config.Conn) ([]int, error) {
	want := strings.Join(t.argv(conn), " ")
	out, err := t.ps()
	if err != nil {
		return nil, fmt.Errorf("process list failed: %w", err)
	}
	var found []int
	for _, line := range strings.Split(string(out), "\n") {
		pid, cmd, _ := strings.Cut(strings.TrimSpace(line), " ")
		if strings.TrimSpace(cmd) != want {
			continue
		}
		if n, err := strconv.Atoi(pid); err == nil {
			found = append(found, n)
		}
	}
	return found, nil
}

type tshDatabase struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Protocol string `json:"protocol"`
	} `json:"spec"`
	Users struct {
		Allowed []string `json:"allowed"`
	} `json:"users"`
}

func (t tsh) Discover() ([]Target, error) {
	cmd := exec.Command(t.tool.Bin, "db", "ls", "--format", "json")
	cmd.Env = t.env()
	out, err := cmd.Output()
	// Only an exit of tsh itself points at the login
	// A missing binary gets the plain cause instead
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee):
		return nil, fmt.Errorf("tsh db ls failed: check the tsh login\n%s", strings.TrimSpace(string(ee.Stderr)))
	case err != nil:
		return nil, fmt.Errorf("tsh db ls couldn't run: %w", err)
	}
	return parseTshDatabases(out)
}

// The name is the discovered-name label when present
// and the resource name otherwise
func parseTshDatabases(out []byte) ([]Target, error) {
	var dbs []tshDatabase
	if err := json.Unmarshal(out, &dbs); err != nil {
		return nil, fmt.Errorf("couldn't parse tsh db ls output: %w", err)
	}
	targets := make([]Target, len(dbs))
	for i, db := range dbs {
		name := db.Metadata.Labels["teleport.internal/discovered-name"]
		if name == "" {
			name = db.Metadata.Name
		}
		targets[i] = Target{Target: db.Metadata.Name, Name: name, Client: tshClients[db.Spec.Protocol], Users: db.Users.Allowed}
	}
	return targets, nil
}
