package sinks_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Started: time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC),
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

// A closed file fails every write like a full disk
func closedFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f
}

func TestJSONLines(t *testing.T) {
	t.Parallel()
	type want struct {
		// What stdout holds before Close
		early       string
		out, stderr string
		closeErr    bool
	}
	type args struct {
		follow bool
		broken bool
		single bool
		runErr error
	}
	lines := "{\"a\":\"café\"}\n{\"raw\":1}\n"
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"one line per record and a count", args{},
			want{out: lines, stderr: "(2 rows)\n"}},
		{"follow flushes every record and a failed run is noted", args{follow: true, runErr: errors.New("x")},
			want{early: lines, out: lines, stderr: "(2 rows, stopped by a failure)\n"}},
		{"a failed output is noted and returned", args{broken: true},
			want{stderr: "(2 rows, stopped by a failure)\n", closeErr: true}},
		{"one record is counted as a row", args{single: true},
			want{out: "{\"a\":\"café\"}\n", stderr: "(1 row)\n"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out, errw bytes.Buffer
			var w io.Writer = &out
			if tc.args.broken {
				w = closedFile(t)
			}
			m := meta
			m.Follow = tc.args.follow
			s, err := sinks.Get("", "arrow", m, w, &errw)
			require.NoError(t, err)
			require.NoError(t, s.Write(r([]string{"a"}, "café")))
			if !tc.args.single {
				raw, ok := s.(sinks.RawWriter)
				require.True(t, ok)
				require.NoError(t, raw.WriteRaw([]byte(`{"raw":1}`)))
			}
			assert.Equal(t, tc.want.early, out.String())
			closeErr := s.Close(tc.args.runErr)
			assert.Equal(t, tc.want.closeErr, closeErr != nil, "%v", closeErr)
			assert.Equal(t, tc.want.out, out.String())
			assert.Equal(t, tc.want.stderr, errw.String())
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
		// Arrow type per column name
		types map[string]string
		// Values of the arrow column s with nil for NULL
		s []any
		// File content in the csv format
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
			types: map[string]string{
				"i": "int64",
				"f": "float64",
				"s": "utf8",
				"b": "bool",
				"o": "extension<arrow.json[storage_type=utf8]>",
			},
			s: []any{"NULL", nil},
		}},
		{
			"csv marks null and writes nested values as JSON",
			"csv",
			[]record.Record{r([]string{"n", "a", "f", "b", "i"}, nil, []any{int64(1), "x"}, 2.0, true, int64(3))},
			nil,
			want{
				complete: true, rows: []float64{1}, csv: "n,a,f,b,i\n\\N,\"[1,\"\"x\"\"]\",2.0,true,3\n",
			},
		},
		{
			"a failed run keeps rows and marks the package incomplete",
			"arrow",
			[]record.Record{r([]string{"a"}, int64(1))},
			errors.New("tool failed"),
			want{
				errText: "tool failed", stderr: "stopped by a failure, saved only what arrived", rows: []float64{1},
			},
		},
		{"an empty csv run writes one empty resource", "csv", nil, nil, want{complete: true, rows: []float64{0}, csv: ""}},
		{"an empty arrow run is a readable stream", "arrow", nil, nil, want{
			complete: true, rows: []float64{0}, types: map[string]string{},
		}},
		{"fields in another order or missing land in their columns", "csv", []record.Record{
			r([]string{"a", "b"}, "1", "2"), r([]string{"b", "a"}, "3", "4"), r([]string{"a"}, "5"),
		}, nil, want{complete: true, rows: []float64{3}, csv: "a,b\n1,2\n4,3\n5,\\N\n"}},
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
			assert.Equal(t, tc.want.errText, text(run["error"]))
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
				assert.Equal(t, `{"version":"0.1.0","alias":"db","client":"mysql","user":"viewer","query":"select 1",`+
					`"limit":10,"write":false,"follow":false,"started":"2026-10-15T10:00:00+00:00"}`, gm)
				assert.Equal(t, tc.want.s, stringColumn(schema, batches, "s"))
			}
			if tc.format == "csv" {
				b, err := os.ReadFile(filepath.Join(dir, "result.csv"))
				require.NoError(t, err)
				assert.Equal(t, tc.want.csv, string(b))
			}
		})
	}
}

func stringColumn(schema *arrow.Schema, batches []arrow.RecordBatch, name string) []any {
	idx := schema.FieldIndices(name)
	if len(idx) == 0 {
		return nil
	}
	var out []any
	for _, b := range batches {
		col := b.Column(idx[0]).(*array.String)
		for j := range col.Len() {
			if col.IsNull(j) {
				out = append(out, nil)
				continue
			}
			out = append(out, col.Value(j))
		}
	}
	return out
}

func text(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// A link to an empty folder and a dangling link are both refused
func links(t *testing.T) (toEmpty, dangling string) {
	t.Helper()
	dir := t.TempDir()
	toEmpty, dangling = filepath.Join(dir, "link"), filepath.Join(dir, "dangling")
	require.NoError(t, os.Symlink(t.TempDir(), toEmpty))
	require.NoError(t, os.Symlink(filepath.Join(dir, "missing"), dangling))
	return toEmpty, dangling
}

func TestGet_Errors(t *testing.T) {
	t.Parallel()
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "x"), nil, 0o600))
	toEmpty, dangling := links(t)
	tcs := []struct {
		name   string
		dir    string
		format string
		err    string
	}{
		{"non-empty folder", full, "arrow", "folder not empty"},
		{"a file in place of the folder", filepath.Join(full, "x"), "csv", "couldn't read the folder"},
		{"unknown format", filepath.Join(t.TempDir(), "out"), "parquet", "unsupported format"},
		{"a link to an empty folder", toEmpty, "arrow", "folder is a symlink"},
		{"a dangling link", dangling, "csv", "folder is a symlink"},
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
	toEmpty, dangling := links(t)
	tcs := []struct {
		name   string
		folder string
		err    string
	}{
		{"stdout needs no folder", "", ""},
		{"missing folder", filepath.Join(t.TempDir(), "out"), ""},
		{"empty folder", t.TempDir(), ""},
		{"non-empty folder", full, "folder not empty"},
		{"a file in place of the folder", filepath.Join(full, "x"), "couldn't read the folder"},
		{"a link to an empty folder", toEmpty, "folder is a symlink"},
		{"a dangling link", dangling, "folder is a symlink"},
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

// An empty folder made by someone else ends up private like a new one
func TestGet_ExistingEmptyFolderBecomesPrivate(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.Chmod(dir, 0o755))
	s, err := sinks.Get(dir, "csv", meta, &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, err)
	require.NoError(t, s.Close(nil))
	fi, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
}
