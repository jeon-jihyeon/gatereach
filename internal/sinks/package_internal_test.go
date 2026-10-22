package sinks

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

var testMeta = Meta{Alias: "db", Client: "mysql", Started: time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)}

func descriptorOf(t *testing.T, dir string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "datapackage.json"))
	require.NoError(t, err)
	var d map[string]any
	require.NoError(t, json.Unmarshal(b, &d))
	return d
}

// With a batch size of 1 the second record brings a new field and starts a new segment
func TestNewFieldStartsSegmentWithoutLoss(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"arrow", "csv"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			p, err := newPackage(dir, format, testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1, time.Hour)
			require.NoError(t, err)
			require.NoError(t, p.Write(record.New([]string{"a"}, []any{int64(1)})))
			require.NoError(t, p.Write(record.New([]string{"a", "b"}, []any{int64(2), "x"})))
			require.NoError(t, p.Close("", nil))
			res := descriptorOf(t, dir)["resources"].([]any)
			require.Len(t, res, 2)
			for i, want := range []string{"result." + Formats[format].suffix, "result-2." + Formats[format].suffix} {
				r := res[i].(map[string]any)
				assert.Equal(t, want, r["path"])
				assert.Equal(t, float64(1), r["gatereach"].(map[string]any)["rows"])
			}
			fields := res[1].(map[string]any)["schema"].(map[string]any)["fields"].([]any)
			require.Len(t, fields, 2)
			assert.Equal(t, "b", fields[1].(map[string]any)["name"])
		})
	}
}

func TestLingerFlushesALoneRecord(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	p, err := newPackage(dir, "csv", testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1000, 50*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close("", nil) })
	require.NoError(t, p.Write(record.New([]string{"a"}, []any{"x"})))
	assert.Eventually(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(dir, "result.csv"))
		return string(b) == "a\nx\n"
	}, 2*time.Second, 50*time.Millisecond)
}

func TestClose_SegmentFailure(t *testing.T) {
	for _, format := range []string{"arrow", "csv"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			p, err := newPackage(dir, format, testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1, time.Hour)
			require.NoError(t, err)
			require.NoError(t, p.Write(record.New([]string{"a"}, []any{"x"})))
			require.NoError(t, p.seg.close())
			assert.Error(t, p.Close("", nil))
			run := descriptorOf(t, dir)["gatereach"].(map[string]any)
			assert.Equal(t, false, run["complete"])
			assert.NotEmpty(t, run["error"])
		})
	}
}

// The folder holds a partial descriptor from the start and only the final one after Close
func TestDescriptorIsPartialUntilClose(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	p, err := newPackage(dir, "csv", testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1000, time.Hour)
	require.NoError(t, err)
	first := descriptorOf(t, dir)
	run := first["gatereach"].(map[string]any)
	assert.Equal(t, false, run["complete"])
	assert.Equal(t, notFinished, run["error"])
	assert.Empty(t, first["resources"])

	require.NoError(t, p.Write(record.New([]string{"a"}, []any{"x"})))
	require.NoError(t, p.Close("", nil))
	final := descriptorOf(t, dir)["gatereach"].(map[string]any)
	assert.Equal(t, true, final["complete"])
	assert.NotContains(t, final, "error")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal(t, []string{"datapackage.json", "result.csv"}, names, "no temp file is left")
	fi, err := os.Stat(filepath.Join(dir, "datapackage.json"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(fileMode), fi.Mode().Perm())
}

// A link planted where a segment goes fails the write and leaves its target alone
// The final descriptor carries the write error
func TestSegmentRefusesAPlantedLink(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
	p, err := newPackage(dir, "csv", testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1, time.Hour)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "result.csv")))
	writeErr := p.Write(record.New([]string{"a"}, []any{"x"}))
	require.Error(t, writeErr)
	assert.Equal(t, writeErr, p.Close("", nil))
	run := descriptorOf(t, dir)["gatereach"].(map[string]any)
	assert.Equal(t, false, run["complete"])
	assert.Equal(t, writeErr.Error(), run["error"])
	b, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "keep", string(b))
}

// A folder swapped for a link after the check is refused when it's opened
func TestMakeFolderRefusesALink(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.Symlink(t.TempDir(), dir))
	assert.ErrorContains(t, makeFolder(dir), "couldn't read the folder")
}
