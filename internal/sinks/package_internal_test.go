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

// 묶음 크기 1로 두 번째 레코드가 새 필드를 가져와 구간을 바꿈
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
			require.NoError(t, p.Close(nil))
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
	t.Cleanup(func() { _ = p.Close(nil) })
	require.NoError(t, p.Write(record.New([]string{"a"}, []any{"x"})))
	assert.Eventually(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(dir, "result.csv"))
		return string(b) == "a\nx\n"
	}, 2*time.Second, 50*time.Millisecond)
}

// 쓰기 실패는 이후 Write와 Close에 남아 호출자가 0이 아닌 코드로 끝냄
func TestWriteErrorSticks(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	p, err := newPackage(dir, "csv", testMeta, &bytes.Buffer{}, &bytes.Buffer{}, 1, time.Hour)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	first := p.Write(record.New([]string{"a"}, []any{"x"}))
	require.Error(t, first)
	assert.Equal(t, first, p.Write(record.New([]string{"a"}, []any{"y"})))
	assert.Error(t, p.Close(nil))
}
