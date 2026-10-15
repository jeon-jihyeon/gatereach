package plugin_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var launcher = filepath.Join("..", "bin", "gatereach")

// 요청 URL의 파일명과 같은 파일이 FAKE_DIR에 있으면 내려받은 것으로 처리
// 없으면 curl의 HTTP 실패 코드
const fakeCurl = `#!/bin/sh
echo "$*" >>"$FAKE_DIR/curl.log"
out=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    https://*) url="$1" ;;
  esac
  shift
done
src="$FAKE_DIR/$(basename "$url")"
[ -f "$src" ] || exit 22
cp "$src" "$out"
`

// FAKE_DIR에 go-ok가 있을 때만 GOBIN에 빌드 결과 작성
const fakeGo = `#!/bin/sh
echo "GOBIN=$GOBIN $*" >>"$FAKE_DIR/go.log"
[ -f "$FAKE_DIR/go-ok" ] || exit 1
printf '#!/bin/sh\necho built "$@"\n' >"$GOBIN/gatereach"
chmod +x "$GOBIN/gatereach"
`

func pluginVersion(t *testing.T) string {
	t.Helper()
	var plugin struct{ Version string }
	b, err := os.ReadFile(filepath.Join("..", ".claude-plugin", "plugin.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &plugin))
	return plugin.Version
}

func writeExec(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(body), 0o700))
}

func archive(t *testing.T, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "gatereach", Mode: 0o755, Size: int64(len(body))}))
	_, err := tw.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

type env struct {
	fake, home, binDir string
}

func (e env) run(t *testing.T, path string, vars ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("/bin/sh", launcher, "ls")
	cmd.Env = append([]string{"PATH=" + path, "FAKE_DIR=" + e.fake}, vars...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode()
	}
	require.NoError(t, err)
	return string(out), 0
}

func TestManifests(t *testing.T) {
	t.Parallel()
	var plugin struct{ Name, Version, Description string }
	b, err := os.ReadFile(filepath.Join("..", ".claude-plugin", "plugin.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &plugin))
	var market struct {
		Plugins []struct{ Name, Source, Description string }
	}
	b, err = os.ReadFile(filepath.Join("..", ".claude-plugin", "marketplace.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &market))
	assert.Equal(t, "gatereach", plugin.Name)
	assert.Regexp(t, `^\d+\.\d+\.\d+$`, plugin.Version)
	require.Len(t, market.Plugins, 1)
	assert.Equal(t, "./", market.Plugins[0].Source)
	assert.Equal(t, plugin.Description, market.Plugins[0].Description)
	info, err := os.Stat(launcher)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&0o111, "the launcher must be executable")
}

func TestLauncher(t *testing.T) {
	t.Parallel()
	version := pluginVersion(t)
	name := fmt.Sprintf("gatereach_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	release := func(t *testing.T, e env, sum string) {
		data := archive(t, "#!/bin/sh\necho fetched \"$@\"\n")
		if sum == "" {
			h := sha256.Sum256(data)
			sum = hex.EncodeToString(h[:])
		}
		require.NoError(t, os.WriteFile(filepath.Join(e.fake, name), data, 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(e.fake, "checksums.txt"), []byte(sum+"  "+name+"\n"), 0o600))
	}
	onPath := func(answer string) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			writeExec(t, filepath.Join(e.fake, "gatereach"), "#!/bin/sh\n[ \"$1\" = --version ] && echo "+answer+" && exit 0\necho path \"$@\"\n")
		}
	}
	miss := func(at int64) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			require.NoError(t, os.MkdirAll(e.binDir, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(e.binDir, ".miss"), []byte(strconv.FormatInt(at, 10)+"\n"), 0o600))
		}
	}
	type want struct {
		code     int
		out      string
		cached   bool
		curl     bool
		missFile bool
	}
	tcs := []struct {
		name    string
		setup   []func(*testing.T, env)
		withBin bool
		want    want
	}{
		{"cached binary runs", []func(*testing.T, env){func(t *testing.T, e env) {
			writeExec(t, filepath.Join(e.binDir, "gatereach"), "#!/bin/sh\necho cached \"$@\"\n")
		}}, false, want{out: "cached ls", cached: true}},
		{"verified release is fetched", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, "") }},
			false, want{out: "fetched ls", cached: true, curl: true}},
		{"checksum mismatch is refused", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, strings.Repeat("0", 64)) }},
			false, want{code: 1, out: "doesn't match checksums.txt", curl: true}},
		{"go install builds into the cache", []func(*testing.T, env){func(t *testing.T, e env) {
			require.NoError(t, os.WriteFile(filepath.Join(e.fake, "go-ok"), nil, 0o600))
		}}, false, want{out: "built ls", cached: true, curl: true}},
		{"PATH build of the plugin version", []func(*testing.T, env){onPath(version)},
			false, want{out: "path ls", curl: true, missFile: true}},
		{"PATH build of another version", []func(*testing.T, env){onPath("9.9.9")},
			false, want{code: 1, out: "on PATH is version 9.9.9", curl: true, missFile: true}},
		{"recent miss skips the network", []func(*testing.T, env){onPath(version), miss(time.Now().Unix())},
			false, want{out: "path ls", missFile: true}},
		{"old miss tries again", []func(*testing.T, env){onPath(version), miss(0)},
			false, want{out: "path ls", curl: true, missFile: true}},
		{"launcher on PATH isn't run as a PATH build", nil,
			true, want{code: 1, out: "couldn't fetch", curl: true, missFile: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			e := env{fake: filepath.Join(root, "fake"), home: filepath.Join(root, "home")}
			e.binDir = filepath.Join(e.home, "bin", "v"+version)
			writeExec(t, filepath.Join(e.fake, "curl"), fakeCurl)
			writeExec(t, filepath.Join(e.fake, "go"), fakeGo)
			for _, s := range tc.setup {
				s(t, e)
			}
			path := e.fake + ":/usr/bin:/bin"
			if tc.withBin {
				bin, err := filepath.Abs(filepath.Dir(launcher))
				require.NoError(t, err)
				path = e.fake + ":" + bin + ":/usr/bin:/bin"
			}
			out, code := e.run(t, path, "GATEREACH_HOME="+e.home, "HOME="+root)
			assert.Equal(t, tc.want.code, code, out)
			assert.Contains(t, out, tc.want.out)
			assert.NotContains(t, out, "on PATH is version unknown")
			_, err := os.Stat(filepath.Join(e.binDir, "gatereach"))
			assert.Equal(t, tc.want.cached, err == nil, "cached binary")
			_, err = os.Stat(filepath.Join(e.fake, "curl.log"))
			assert.Equal(t, tc.want.curl, err == nil, "curl called")
			_, err = os.Stat(filepath.Join(e.binDir, ".miss"))
			assert.Equal(t, tc.want.missFile, err == nil, ".miss")
			if tc.want.out == "built ls" {
				log, err := os.ReadFile(filepath.Join(e.fake, "go.log"))
				require.NoError(t, err)
				assert.Contains(t, string(log), "GOBIN="+filepath.Join(e.binDir, ".build."), "go install writes into a temp folder")
			}
			left, err := filepath.Glob(filepath.Join(e.binDir, ".[fb]*.*"))
			require.NoError(t, err)
			assert.Empty(t, left, "temp folders are removed")
		})
	}
}

func TestLauncherNoHome(t *testing.T) {
	t.Parallel()
	out, code := env{fake: t.TempDir()}.run(t, "/usr/bin:/bin")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "neither GATEREACH_HOME nor HOME is set")
}
