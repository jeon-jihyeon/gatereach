// Package version은 빌드 버전
package version

import (
	"runtime/debug"
	"strings"
)

// 릴리스 빌드는 `-ldflags -X`로 앞의 v를 뺀 태그 값 주입
// 코드에 버전 번호를 두지 않아 plugin.json과 어긋날 수 없음
var Version = ""

func init() {
	if Version == "" {
		Version = fromBuildInfo()
	}
}

// go install로 받은 빌드는 모듈 버전 사용
// 로컬 빌드는 dev
func fromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || !strings.HasPrefix(info.Main.Version, "v") {
		return "dev"
	}
	return strings.TrimPrefix(info.Main.Version, "v")
}
