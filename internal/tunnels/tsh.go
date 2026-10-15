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

// Teleport 프로토콜 → gatereach client
var tshClients = map[string]string{"mysql": "mysql", "redis": "redis"}

type tsh struct {
	tool config.Tool
	// 내 계정의 프로세스 목록
	ps func() ([]byte, error)
}

func newTsh(tool config.Tool) Kind {
	return tsh{tool: tool, ps: listProcesses}
}

func listProcesses() ([]byte, error) {
	return exec.Command("ps", "-ww", "-U", strconv.Itoa(os.Getuid()), "-o", "pid=,command=").Output()
}

// Teleport Connect 앱이 보관한 프로필을 쓰려면 TELEPORT_HOME 지정
func (t tsh) env() []string {
	if t.tool.Home == "" {
		return os.Environ()
	}
	return append(os.Environ(), "TELEPORT_HOME="+t.tool.Home)
}

func (t tsh) argv(conn config.Conn) []string {
	return []string{t.tool.Bin, "proxy", "db", "--tunnel", "--port", strconv.Itoa(conn.Port), "--db-user", conn.User, conn.Tunnel.Target}
}

func (t tsh) Command(conn config.Conn) ([]string, []string) {
	return t.argv(conn), t.env()
}

// Teleport Connect 앱 같은 다른 프로세스가 연 터널은 명령줄이 달라 제외
func (t tsh) Pids(conn config.Conn) ([]int, error) {
	want := strings.Join(t.argv(conn), " ")
	out, err := t.ps()
	if err != nil {
		return nil, fmt.Errorf("프로세스 목록 조회 실패: %w", err)
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
	if err != nil {
		detail := err.Error()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			detail = strings.TrimSpace(string(ee.Stderr))
		}
		return nil, fmt.Errorf("tsh db ls 실패: tsh 로그인 확인\n%s", detail)
	}
	return parseTshDatabases(out)
}

// 이름은 discovered-name 라벨을 먼저 쓰고 없으면 리소스 이름
func parseTshDatabases(out []byte) ([]Target, error) {
	var dbs []tshDatabase
	if err := json.Unmarshal(out, &dbs); err != nil {
		return nil, fmt.Errorf("tsh db ls 출력 해석 실패: %w", err)
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
