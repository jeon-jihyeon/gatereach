// Package tunnels는 필요할 때 열고 닫을 때까지 유지하는 터널
package tunnels

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=kind_mock_test.go -package=tunnels -self_package=github.com/jeon-jihyeon/gatereach/internal/tunnels -mock_names=Kind=MockKind github.com/jeon-jihyeon/gatereach/internal/tunnels Kind

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/jeon-jihyeon/gatereach/internal/config"
	"github.com/jeon-jihyeon/gatereach/internal/proc"
)

// 상태 파일 없이 프로세스 목록이 곧 상태
type Kind interface {
	// 터널 프로세스의 명령줄과 환경
	Command(conn config.Conn) (argv, env []string)
	// 내 계정 소유이고 명령줄이 정확히 일치하는 프로세스
	Pids(conn config.Conn) ([]int, error)
	Discover() ([]Target, error)
}

// 터널로 닿을 수 있는 대상 하나
type Target struct {
	Target string
	Name   string
	// gatereach client 이름
	// 모르는 프로토콜이면 빈 값
	Client string
	Users  []string
}

// 종류 이름별 생성 함수
// 설정 파일의 `[tunnels.<종류>]` 값을 받음
var Kinds = map[string]func(config.Tool) Kind{"tsh": newTsh}

func Names() []string {
	out := make([]string, 0, len(Kinds))
	for k := range Kinds {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// 터널 열기와 닫기 결과
type Result string

const (
	Already Result = "already" // 내가 연 터널이 이미 열림
	Foreign Result = "foreign" // 다른 프로세스가 그 포트를 써 같은 대상인지 알 수 없음
	Opened  Result = "opened"  // 새로 엶
	Closed  Result = "closed"  // 닫음
	None    Result = "none"    // 열린 것 없음
)

const (
	openWait = 20 * time.Second
	openTick = 300 * time.Millisecond
	dialWait = 300 * time.Millisecond
)

// 설정의 종류 이름으로 터널 종류 생성
// 터널 설정이 없으면 nil
func New(cfg *config.Config, conn config.Conn) (Kind, error) {
	if conn.Tunnel == nil {
		return nil, nil
	}
	newKind, ok := Kinds[conn.Tunnel.Type]
	if !ok {
		return nil, fmt.Errorf("지원하지 않는 터널: %s", conn.Tunnel.Type)
	}
	return newKind(cfg.Tool(conn.Tunnel.Type, conn.Tunnel.Type)), nil
}

// 포트 없는 연결은 ok false
// 이름 해석 실패 같은 오류도 닫힘
func IsOpen(conn config.Conn) (open bool, ok bool) {
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

// 같은 별칭을 동시에 열면 두 번째 터널이 포트를 잡지 못하고 끝나므로 잠금 안에서만 띄움
func open(k Kind, alias string, conn config.Conn, wait, tick time.Duration) (Result, error) {
	if up, _ := IsOpen(conn); up {
		return holder(k, conn)
	}
	if k == nil {
		return "", fmt.Errorf("%s 닫혀 있고 터널 설정 없음: %s:%d", alias, conn.Host, conn.Port)
	}
	unlock, err := proc.Lock(config.LogPath(alias, ".lock"))
	if err != nil {
		return "", err
	}
	defer unlock()
	// 잠금을 기다리는 동안 다른 gatereach가 연 터널
	if up, _ := IsOpen(conn); up {
		return holder(k, conn)
	}
	argv, env := k.Command(conn)
	path := config.LogPath(alias, ".log")
	ready := func() bool { up, _ := IsOpen(conn); return up }
	err = proc.Spawn(argv, env, path, ready, wait, tick)
	var exit *proc.ExitError
	switch {
	case err == nil:
		return Opened, nil
	case errors.As(err, &exit):
		return "", fmt.Errorf("%s 열기 실패: 터널 %s\n%s\n로그: %s", alias, exit, proc.Tail(path, 5), path)
	case errors.Is(err, proc.ErrTimeout):
		return "", fmt.Errorf("%s 열기 실패: %s 안에 포트가 열리지 않음\n터널 로그인 상태 확인\n%s\n로그: %s", alias, wait, proc.Tail(path, 5), path)
	}
	return "", fmt.Errorf("%s 터널 실행 실패: %w", alias, err)
}

// 열린 포트를 쥔 쪽
// 터널 설정이 없으면 직접 연결
func holder(k Kind, conn config.Conn) (Result, error) {
	if k == nil {
		return Already, nil
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
	return closeTunnel(k, conn)
}

// 터널 설정이 없는 직접 연결은 닫을 터널이 없음
func closeTunnel(k Kind, conn config.Conn) (Result, error) {
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
		return Closed, nil
	}
	if up, _ := IsOpen(conn); up {
		return Foreign, nil
	}
	return None, nil
}
