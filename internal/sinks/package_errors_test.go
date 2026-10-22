package sinks_test

import (
	"bytes"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/sinks"
)

func TestGet_WriteFailureSticks(t *testing.T) {
	for _, format := range []string{"arrow", "csv"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			saved := dir + "-saved"
			s, err := sinks.Get(dir, format, meta, &bytes.Buffer{}, &bytes.Buffer{})
			require.NoError(t, err)
			require.NoError(t, os.Rename(dir, saved))
			require.NoError(t, os.WriteFile(dir, []byte("blocked"), 0o600))
			var first error
			for range 1000 {
				first = s.Write(r([]string{"value"}, "ann"))
			}
			require.Error(t, first)
			assert.Equal(t, first, s.Write(r([]string{"value"}, "ann")))
			require.NoError(t, os.Remove(dir))
			require.NoError(t, os.Rename(saved, dir))
			assert.Equal(t, first, s.Close("", nil))
			run := readDescriptor(t, dir)["gatereach"].(map[string]any)
			assert.Equal(t, false, run["complete"])
			assert.Equal(t, first.Error(), run["error"])
		})
	}
}

func TestGet_DescriptorTempFailure(t *testing.T) {
	for _, format := range []string{"arrow", "csv"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			saved := dir + "-saved"
			var out bytes.Buffer
			s, err := sinks.Get(dir, format, meta, &out, &bytes.Buffer{})
			require.NoError(t, err)
			for range 1000 {
				require.NoError(t, s.Write(r([]string{"value"}, "ann")))
			}
			require.NoError(t, os.Rename(dir, saved))
			require.NoError(t, os.WriteFile(dir, []byte("blocked"), 0o600))
			err = s.Close("", nil)
			assert.ErrorIs(t, err, syscall.ENOTDIR)
			assert.Empty(t, out.String())
			run := readDescriptor(t, saved)["gatereach"].(map[string]any)
			assert.Equal(t, false, run["complete"])
			assert.Equal(t, "not finished", run["error"])
			temps, err := filepath.Glob(filepath.Join(saved, ".datapackage-*.json"))
			require.NoError(t, err)
			assert.Empty(t, temps)
		})
	}
}

// Folder work comes before any format work so one format covers both
func TestNewPackage_FolderErrors(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Symlink("missing", filepath.Join(root, "link")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), []byte("blocked"), 0o600))
	for _, tc := range []struct {
		name   string
		folder string
		want   error
	}{
		{"empty path", "", syscall.ENOENT},
		{"dangling parent", filepath.Join(root, "link", "out"), syscall.EEXIST},
		{"under a file", filepath.Join(root, "file", "out"), syscall.ENOTDIR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := sinks.NewPackage(tc.folder, "csv", meta, &bytes.Buffer{}, &bytes.Buffer{})
			assert.ErrorIs(t, err, tc.want)
			assert.Nil(t, s)
		})
	}
}

// The limits apply in a child process so the test binary keeps its own
func TestNewPackage_InitialDescriptorFailure(t *testing.T) {
	if mode := os.Getenv("GATEREACH_TEST_SINK_FAILURE"); mode != "" {
		var files []*os.File
		if mode == "write" {
			signal.Ignore(syscall.SIGXFSZ)
			limit(t, syscall.RLIMIT_FSIZE, 0)
		} else {
			limit(t, syscall.RLIMIT_NOFILE, 64)
			for {
				file, err := os.Open(os.DevNull)
				if err != nil {
					require.ErrorIs(t, err, syscall.EMFILE)
					break
				}
				files = append(files, file)
			}
		}
		s, err := sinks.NewPackage(os.Getenv("GATEREACH_TEST_SINK_FOLDER"), "csv", meta, &bytes.Buffer{}, &bytes.Buffer{})
		for _, file := range files {
			require.NoError(t, file.Close())
		}
		assert.ErrorIs(t, err, map[string]error{"create": syscall.EMFILE, "write": syscall.EFBIG}[mode])
		assert.Nil(t, s)
		return
	}
	for _, tc := range []struct {
		name string
		mode string
		file bool
	}{
		{"descriptor cannot open", "create", false},
		{"descriptor cannot write", "write", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			command := exec.Command(os.Args[0], "-test.run=^TestNewPackage_InitialDescriptorFailure$")
			command.Env = append(os.Environ(), "GATEREACH_TEST_SINK_FAILURE="+tc.mode, "GATEREACH_TEST_SINK_FOLDER="+dir)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
			assert.DirExists(t, dir)
			_, err = os.Stat(filepath.Join(dir, "datapackage.json"))
			assert.Equal(t, tc.file, err == nil)
		})
	}
}

// The file size limit applies in a child process so the test binary keeps its own
func TestGet_DescriptorWriteFailure(t *testing.T) {
	if dir := os.Getenv("GATEREACH_TEST_SINK_FOLDER"); dir != "" {
		s, err := sinks.Get(dir, "csv", meta, &bytes.Buffer{}, &bytes.Buffer{})
		require.NoError(t, err)
		signal.Ignore(syscall.SIGXFSZ)
		limit(t, syscall.RLIMIT_FSIZE, 0)
		assert.ErrorIs(t, s.Close("", nil), syscall.EFBIG)
		run := readDescriptor(t, dir)["gatereach"].(map[string]any)
		assert.Equal(t, false, run["complete"])
		assert.Equal(t, "not finished", run["error"])
		temps, err := filepath.Glob(filepath.Join(dir, ".datapackage-*.json"))
		require.NoError(t, err)
		assert.Empty(t, temps)
		return
	}
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	command := exec.Command(os.Args[0], "-test.run=^TestGet_DescriptorWriteFailure$")
	command.Env = append(os.Environ(), "GATEREACH_TEST_SINK_FOLDER="+dir)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func limit(t *testing.T, resource int, maximum uint64) {
	t.Helper()
	var original syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(resource, &original))
	limited := original
	limited.Cur = min(original.Cur, maximum)
	require.NoError(t, syscall.Setrlimit(resource, &limited))
	t.Cleanup(func() { require.NoError(t, syscall.Setrlimit(resource, &original)) })
}
