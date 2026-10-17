package plugin_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
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
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var launcher = filepath.Join("..", "bin", "gatereach")

// A file in FAKE_DIR named like the requested URL counts as downloaded
// Otherwise it exits with curl's HTTP failure code
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

// Writes a build to GOBIN only when FAKE_DIR holds go-ok
const fakeGo = `#!/bin/sh
echo "GOBIN=$GOBIN $*" >>"$FAKE_DIR/go.log"
[ -f "$FAKE_DIR/go-ok" ] || exit 1
printf '#!/bin/sh\necho built "$@"\n' >"$GOBIN/gatereach"
chmod +x "$GOBIN/gatereach"
`

// Passes unless FAKE_DIR holds gh-fail
// gh-logout fails the login check
const fakeGh = `#!/bin/sh
echo "$*" >>"$FAKE_DIR/gh.log"
[ "$1" = auth ] && [ -f "$FAKE_DIR/gh-logout" ] && exit 1
[ "$1" = auth ] && exit 0
[ -f "$FAKE_DIR/gh-fail" ] && echo "no attestation" >&2 && exit 1
exit 0
`

// A folder that links every program in /usr/bin and /bin except gh
// CI runners ship gh in /usr/bin so a plain system PATH can't leave it out
func systemTools(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, sys := range []string{"/usr/bin", "/bin"} {
		entries, err := os.ReadDir(sys)
		require.NoError(t, err)
		for _, e := range entries {
			if e.Name() == "gh" {
				continue
			}
			link := filepath.Join(dir, e.Name())
			if _, err := os.Lstat(link); err == nil {
				continue
			}
			require.NoError(t, os.Symlink(filepath.Join(sys, e.Name()), link))
		}
	}
	return dir
}

func readManifest(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", ".claude-plugin", name))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, v))
}

func pluginVersion(t *testing.T) string {
	t.Helper()
	var plugin struct{ Version string }
	readManifest(t, "plugin.json", &plugin)
	return plugin.Version
}

// Empty when the file doesn't exist
func readOrEmpty(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
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
	// Working directory of the launcher
	dir string
}

func (e env) run(t *testing.T, path string, vars ...string) (string, int) {
	t.Helper()
	// Absolute since the working directory changes
	abs, err := filepath.Abs(launcher)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", abs, "ls")
	cmd.Env = append([]string{"PATH=" + path, "FAKE_DIR=" + e.fake}, vars...)
	cmd.Dir = e.dir
	// A launcher that keeps starting itself is stopped with every process it started
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
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
	readManifest(t, "plugin.json", &plugin)
	var market struct {
		Plugins []struct{ Name, Source, Description string }
	}
	readManifest(t, "marketplace.json", &market)
	assert.Equal(t, "gatereach", plugin.Name)
	assert.Regexp(t, `^\d+\.\d+\.\d+$`, plugin.Version)
	require.Len(t, market.Plugins, 1)
	assert.Equal(t, plugin.Name, market.Plugins[0].Name)
	assert.Equal(t, "./", market.Plugins[0].Source)
	assert.Equal(t, plugin.Description, market.Plugins[0].Description)
	info, err := os.Stat(launcher)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&0o111, "the launcher must be executable")
}

func TestLauncher(t *testing.T) {
	t.Parallel()
	version := pluginVersion(t)
	tools := systemTools(t)
	name := fmt.Sprintf("gatereach_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	// An empty sum means the archive's real checksum
	release := func(t *testing.T, e env, sum string) {
		t.Helper()
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
	gh := func(fail bool) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			writeExec(t, filepath.Join(e.fake, "gh"), fakeGh)
			if fail {
				require.NoError(t, os.WriteFile(filepath.Join(e.fake, "gh-fail"), nil, 0o600))
			}
		}
	}
	// A gatereach of the plugin version in the working directory
	inWorkDir := func(t *testing.T, e env) {
		writeExec(t, filepath.Join(e.dir, "gatereach"), "#!/bin/sh\necho "+version+"\n")
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
		// Part of the GOBIN go install got
		goLog string
		// Part of the arguments gh got
		ghLog string
	}
	tcs := []struct {
		name    string
		setup   []func(*testing.T, env)
		withBin bool
		// Entries put before the fake and system folders
		pathHead string
		want     want
	}{
		{"cached binary runs", []func(*testing.T, env){func(t *testing.T, e env) {
			writeExec(t, filepath.Join(e.binDir, "gatereach"), "#!/bin/sh\necho cached \"$@\"\n")
		}}, false, "", want{out: "cached ls", cached: true}},
		{"verified release is fetched", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, "") }},
			false, "", want{out: "fetched ls", cached: true, curl: true}},
		{"attested release is fetched", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, "") }, gh(false)},
			false, "", want{out: "fetched ls", cached: true, curl: true, ghLog: name + " --repo jeon-jihyeon/gatereach"}},
		{"release without an attestation is refused", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, "") }, gh(true)},
			false, "", want{code: 1, out: "gh attestation verify failed", curl: true, ghLog: "attestation verify"}},
		{"gh without a login checks only the checksum", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, "") }, gh(true),
			func(t *testing.T, e env) {
				require.NoError(t, os.WriteFile(filepath.Join(e.fake, "gh-logout"), nil, 0o600))
			}},
			false, "", want{out: "fetched ls", cached: true, curl: true, ghLog: "auth status"}},
		{"checksum mismatch is refused", []func(*testing.T, env){func(t *testing.T, e env) { release(t, e, strings.Repeat("0", 64)) }},
			false, "", want{code: 1, out: "doesn't match checksums.txt", curl: true}},
		{"go install builds into the cache", []func(*testing.T, env){func(t *testing.T, e env) {
			require.NoError(t, os.WriteFile(filepath.Join(e.fake, "go-ok"), nil, 0o600))
		}}, false, "", want{out: "built ls", cached: true, curl: true, goLog: filepath.Join("bin", "v"+version, ".build.")}},
		{"PATH build of the plugin version", []func(*testing.T, env){onPath(version)},
			false, "", want{out: "path ls", curl: true, missFile: true}},
		{"PATH build of another version", []func(*testing.T, env){onPath("9.9.9")},
			false, "", want{code: 1, out: "on PATH is version 9.9.9", curl: true, missFile: true}},
		{"recent miss skips the network", []func(*testing.T, env){onPath(version), miss(time.Now().Unix())},
			false, "", want{out: "path ls", missFile: true}},
		{"old miss tries again", []func(*testing.T, env){onPath(version), miss(0)},
			false, "", want{out: "path ls", curl: true, missFile: true}},
		{"launcher on PATH isn't run as a PATH build", nil,
			true, "", want{code: 1, out: "couldn't fetch", curl: true, missFile: true}},
		{"PATH build after the launcher on PATH", []func(*testing.T, env){onPath(version)},
			true, "", want{out: "path ls", curl: true, missFile: true}},
		{"empty and relative PATH entries are skipped", []func(*testing.T, env){inWorkDir},
			false, "::.:", want{code: 1, out: "couldn't fetch", curl: true, missFile: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			e := env{fake: filepath.Join(root, "fake"), home: filepath.Join(root, "home"), dir: filepath.Join(root, "work")}
			require.NoError(t, os.MkdirAll(e.dir, 0o700))
			e.binDir = filepath.Join(e.home, "bin", "v"+version)
			writeExec(t, filepath.Join(e.fake, "curl"), fakeCurl)
			writeExec(t, filepath.Join(e.fake, "go"), fakeGo)
			for _, s := range tc.setup {
				s(t, e)
			}
			path := tc.pathHead + e.fake + ":" + tools
			// The launcher's own directory comes first so command -v alone would find only the launcher
			if tc.withBin {
				bin, err := filepath.Abs(filepath.Dir(launcher))
				require.NoError(t, err)
				path = bin + ":" + path
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
			assert.Contains(t, readOrEmpty(filepath.Join(e.fake, "go.log")), tc.want.goLog, "go install writes into a temp folder")
			assert.Contains(t, readOrEmpty(filepath.Join(e.fake, "gh.log")), tc.want.ghLog, "gh attestation verify")
			if tc.want.curl {
				assert.Contains(t, readOrEmpty(filepath.Join(e.fake, "curl.log")), "--proto =https --tlsv1.2", "curl stays on HTTPS")
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

// go install refuses a relative GOBIN
func TestLauncherRelativeHome(t *testing.T) {
	t.Parallel()
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	e := env{fake: filepath.Join(root, "fake"), dir: root}
	writeExec(t, filepath.Join(e.fake, "curl"), fakeCurl)
	writeExec(t, filepath.Join(e.fake, "go"), fakeGo)
	require.NoError(t, os.WriteFile(filepath.Join(e.fake, "go-ok"), nil, 0o600))

	out, code := e.run(t, e.fake+":"+systemTools(t), "GATEREACH_HOME=home", "HOME="+root)
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "built ls")
	assert.Contains(t, readOrEmpty(filepath.Join(e.fake, "go.log")), "GOBIN="+filepath.Join(root, "home", "bin", "v"+pluginVersion(t)))
}

// Each copy would otherwise find the other as the PATH build and ask it for its version without end
func TestLauncherAnotherCopyOnPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	e := env{fake: filepath.Join(root, "fake"), home: filepath.Join(root, "home"), dir: root}
	writeExec(t, filepath.Join(e.fake, "curl"), fakeCurl)
	writeExec(t, filepath.Join(e.fake, "go"), fakeGo)
	body, err := os.ReadFile(launcher)
	require.NoError(t, err)
	manifest, err := os.ReadFile(filepath.Join("..", ".claude-plugin", "plugin.json"))
	require.NoError(t, err)
	copyBin := filepath.Join(root, "copy", "bin")
	writeExec(t, filepath.Join(copyBin, "gatereach"), string(body))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "copy", ".claude-plugin"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "copy", ".claude-plugin", "plugin.json"), manifest, 0o600))
	bin, err := filepath.Abs(filepath.Dir(launcher))
	require.NoError(t, err)

	out, code := e.run(t, bin+":"+copyBin+":"+e.fake+":"+systemTools(t), "GATEREACH_HOME="+e.home, "HOME="+root)
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, filepath.Join(copyBin, "gatereach")+" on PATH is version unknown")
	assert.Contains(t, out, "couldn't fetch")
}
