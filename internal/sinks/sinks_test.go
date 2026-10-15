package sinks_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/record"
	"github.com/jeon-jihyeon/gatereach/internal/sinks"
)

var meta = sinks.Meta{
	Version: "0.1.0", Alias: "db", Client: "mysql", User: "viewer", Query: "select 1", Limit: 10,
	Started: time.Date(2026, 10, 15, 10, 0, 0, 0, time.FixedZone("KST", 9*3600)),
}

func r(names []string, values ...any) record.Record { return record.New(names, values) }

func readDescriptor(t *testing.T, dir string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "datapackage.json"))
	require.NoError(t, err)
	var d map[string]any
	require.NoError(t, json.Unmarshal(b, &d))
	return d
}

func readArrow(t *testing.T, path string) (*arrow.Schema, []arrow.RecordBatch) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	rd, err := ipc.NewReader(f)
	require.NoError(t, err)
	var out []arrow.RecordBatch
	for rd.Next() {
		b := rd.RecordBatch()
		b.Retain()
		t.Cleanup(b.Release)
		out = append(out, b)
	}
	require.NoError(t, rd.Err())
	return rd.Schema(), out
}

func TestJSONLines(t *testing.T) {
	t.Parallel()
	type want struct {
		out, err string
	}
	tcs := []struct {
		name   string
		follow bool
		runErr error
		want   want
	}{
		{"one line per record and a count", false, nil, want{"{\"a\":\"한글\"}\n", "(1 rows)\n"}},
		{"a failure is noted", true, errors.New("x"), want{"{\"a\":\"한글\"}\n", "(1 rows, 실패로 중단)\n"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out, errw bytes.Buffer
			m := meta
			m.Follow = tc.follow
			s, err := sinks.Get("", "arrow", m, &out, &errw)
			require.NoError(t, err)
			require.NoError(t, s.Write(r([]string{"a"}, "한글")))
			require.NoError(t, s.Close(tc.runErr))
			assert.Equal(t, tc.want.out, out.String())
			assert.Equal(t, tc.want.err, errw.String())
		})
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	names := []string{"i", "f", "s", "b", "o"}
	typed := r(names, int64(1), 1.5, "NULL", true, map[string]any{"k": int64(1)})
	nulls := r(names, nil, nil, nil, nil, nil)
	type want struct {
		complete bool
		errText  string
		stderr   string
		rows     []float64
		// arrow 형식에서 열 이름별 타입
		types map[string]string
		// csv 형식의 파일 내용
		csv string
	}
	tcs := []struct {
		name    string
		format  string
		records []record.Record
		runErr  error
		want    want
	}{
		{"arrow keeps types and nulls", "arrow", []record.Record{typed, nulls}, nil, want{
			complete: true, rows: []float64{2},
			types: map[string]string{"i": "int64", "f": "float64", "s": "utf8", "b": "bool", "o": "extension<arrow.json[storage_type=utf8]>"},
		}},
		{"csv marks null and writes nested values as JSON", "csv", []record.Record{r([]string{"n", "a", "f"}, nil, []any{int64(1), "x"}, 2.0)}, nil, want{
			complete: true, rows: []float64{1}, csv: "n,a,f\n\\N,\"[1,\"\"x\"\"]\",2.0\n",
		}},
		{"a failed run keeps rows and marks the package incomplete", "arrow", []record.Record{r([]string{"a"}, int64(1))}, errors.New("tool failed"), want{
			errText: "tool failed", stderr: "실패로 중단되어 받은 데까지만 저장", rows: []float64{1},
		}},
		{"an empty run still writes one resource", "csv", nil, nil, want{complete: true, rows: []float64{0}, csv: "\n"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			var out, errw bytes.Buffer
			s, err := sinks.Get(dir, tc.format, meta, &out, &errw)
			require.NoError(t, err)
			for _, rec := range tc.records {
				require.NoError(t, s.Write(rec))
			}
			require.NoError(t, s.Close(tc.runErr))

			d := readDescriptor(t, dir)
			run := d["gatereach"].(map[string]any)
			assert.Equal(t, tc.want.complete, run["complete"])
			assert.Equal(t, tc.want.errText, or(run["error"]))
			assert.Contains(t, errw.String(), tc.want.stderr)
			if tc.want.complete {
				assert.Equal(t, filepath.Join(dir, "datapackage.json")+"\n", out.String())
			}
			res := d["resources"].([]any)
			require.Len(t, res, len(tc.want.rows))
			for i, rows := range tc.want.rows {
				assert.Equal(t, rows, res[i].(map[string]any)["gatereach"].(map[string]any)["rows"])
			}
			for _, p := range []string{dir, filepath.Join(dir, "datapackage.json")} {
				fi, err := os.Stat(p)
				require.NoError(t, err)
				assert.Zero(t, fi.Mode().Perm()&0o077, "%s is private", p)
			}
			if tc.want.types != nil {
				schema, batches := readArrow(t, filepath.Join(dir, "result.arrows"))
				types := map[string]string{}
				for _, f := range schema.Fields() {
					types[f.Name] = f.Type.String()
				}
				assert.Equal(t, tc.want.types, types)
				gm, _ := schema.Metadata().GetValue("gatereach")
				assert.Equal(t, `{"version":"0.1.0","alias":"db","client":"mysql","user":"viewer","query":"select 1","limit":10,"write":false,"follow":false,"started":"2026-10-15T10:00:00+09:00"}`, gm)
				col := batches[0].Column(2).(*array.String)
				assert.Equal(t, "NULL", col.Value(0))
				assert.True(t, col.IsNull(1), "NULL apart from the string NULL")
			}
			if tc.want.csv != "" {
				b, err := os.ReadFile(filepath.Join(dir, "result.csv"))
				require.NoError(t, err)
				assert.Equal(t, tc.want.csv, string(b))
			}
		})
	}
}

func or(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func TestGet_에러(t *testing.T) {
	t.Parallel()
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "x"), nil, 0o600))
	tcs := []struct {
		name   string
		dir    string
		format string
		err    string
	}{
		{"non-empty folder", full, "arrow", "비어 있지 않은 폴더"},
		{"unknown format", filepath.Join(t.TempDir(), "out"), "parquet", "지원하지 않는 형식"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := sinks.Get(tc.dir, tc.format, meta, &bytes.Buffer{}, &bytes.Buffer{})
			assert.ErrorContains(t, err, tc.err)
		})
	}
}

func TestCheckFolder(t *testing.T) {
	t.Parallel()
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "x"), nil, 0o600))
	tcs := []struct {
		name   string
		folder string
		err    string
	}{
		{"stdout needs no folder", "", ""},
		{"missing folder", filepath.Join(t.TempDir(), "out"), ""},
		{"empty folder", t.TempDir(), ""},
		{"non-empty folder", full, "비어 있지 않은 폴더"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := sinks.CheckFolder(tc.folder)
			assert.Equal(t, tc.err == "", err == nil, "%v", err)
			assert.Contains(t, fmt.Sprint(err), tc.err)
		})
	}
}
