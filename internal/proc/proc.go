// Package proc는 gatereach가 끝나도 남는 자식 프로세스를 띄우고 준비될 때까지 대기
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

// 시간 초과로 SIGTERM을 보낸 뒤 SIGKILL까지 기다리는 시간
const killWait = 5 * time.Second

// 준비되기 전에 자식이 끝남
type ExitError struct {
	Code int
}

func (e *ExitError) Error() string { return fmt.Sprintf("종료 코드 %d", e.Code) }

// 대기 시간 안에 준비되지 않아 자식을 종료함
var ErrTimeout = errors.New("시간 초과")

// 새 프로세스 세션으로 띄워 부모와 셸의 Ctrl-C가 닿지 않음
// stdout과 stderr는 매번 비운 로그 파일로 받아 크기를 한 번의 수명으로 제한
// ready가 참이면 자식을 기다리지 않고 반환
// 시간이 지나면 자식의 프로세스 그룹에 SIGTERM
// 그래도 남으면 SIGKILL해 자식이 기다리는 손자 프로세스까지 정리
func Spawn(argv, env []string, logPath string, ready func() bool, wait, tick time.Duration) error {
	return spawn(argv, env, logPath, ready, wait, tick, killWait)
}

func spawn(argv, env []string, logPath string, ready func() bool, wait, tick, grace time.Duration) error {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return err
	}
	log, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer log.Close()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.Stdout = log
	cmd.Stderr = log
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
			return nil
		}
		select {
		case <-exited:
			return &ExitError{Code: cmd.ProcessState.ExitCode()}
		case <-time.After(tick):
		}
	}
	// Setsid로 자식이 그룹 대표라 음수 pid가 그룹 전체
	group := -cmd.Process.Pid
	_ = syscall.Kill(group, syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(grace):
		_ = syscall.Kill(group, syscall.SIGKILL)
		<-exited
	}
	return ErrTimeout
}

// 같은 경로를 잡은 다른 프로세스가 놓을 때까지 기다려 배타 잠금
// 프로세스가 끝나면 커널이 풀어 상태 파일이 아님
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
	// 파일을 닫으면 flock도 풀림
	return func() { _ = f.Close() }, nil
}

// 실패 원인을 보여 줄 로그 끝 n줄
func Tail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
