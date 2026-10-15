// Package secrets는 설정의 비밀값 참조 해석
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// 항목이 없을 때 security의 종료 코드
const keychainMissing = 44

// 설정 파일에는 비밀값 대신 참조만 저장
// 1. keychain:항목 → macOS Keychain
// 2. env:변수 → 환경변수
// 값은 드라이버 설정에만 넘겨 파일과 명령 인자에 남지 않음
// Keychain 확인 창이 떠 있어도 ctx가 끝나면 security를 멈춤
func Resolve(ctx context.Context, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	kind, name, _ := strings.Cut(ref, ":")
	switch kind {
	case "keychain":
		out, err := exec.CommandContext(ctx, "security", "find-generic-password", "-s", name, "-w").Output()
		var exit *exec.ExitError
		switch {
		case err == nil:
			return strings.TrimRight(string(out), "\n"), nil
		case errors.As(err, &exit) && exit.ExitCode() == keychainMissing:
			return "", fmt.Errorf("Keychain 항목 없음: %s", name)
		}
		return "", fmt.Errorf("Keychain 조회 실패: %s: %w", name, err)
	case "env":
		v, ok := os.LookupEnv(name)
		if !ok {
			return "", fmt.Errorf("환경변수 없음: %s", name)
		}
		return v, nil
	}
	return "", fmt.Errorf("지원하지 않는 secret 참조: %s", ref)
}
