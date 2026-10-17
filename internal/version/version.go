// Package version holds the build version
package version

import (
	"runtime/debug"
	"strings"
)

// Release builds inject the tag without its leading v through `-ldflags -X`
// No version number lives in the code so it can't drift from plugin.json
var Version string

func init() {
	if Version == "" {
		Version = fromBuildInfo()
	}
}

// 1. A build from go install uses the module version
// 2. A local build is dev even though Go stamps it with the checkout's tag and VCS state
func fromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || !strings.HasPrefix(info.Main.Version, "v") {
		return "dev"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return "dev"
		}
	}
	return strings.TrimPrefix(info.Main.Version, "v")
}
