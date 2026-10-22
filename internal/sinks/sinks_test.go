package sinks_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
			closeErr := s.Close("", tc.args.runErr)
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
	startKeys := []string{"alias", "client", "user", "limit", "write", "follow", "started", "query"}
	budgetKeys := []string{"alias", "client", "user", "limit", "max", "unit", "write", "follow", "started", "query"}
	startMeta := `"alias":"db","client":"mysql","user":"viewer","limit":10,` +
		`"write":false,"follow":false,"started":"2026-10-15T10:00:00+00:00","query":"select 1"}`
	type args struct {
		format  string
		records []record.Record
		max     float64
		unit    string
		stop    sinks.Stop
		runErr  error
	}
	type want struct {
		complete bool
		errText  string
		stopped  string
		// Keys of the gatereach object in the descriptor in file order
		keys   []string
		stderr string
		rows   []float64
		// Arrow type per column name
		types map[string]string
		// The gatereach value in the Arrow schema metadata
		meta string
		// Values of the arrow column s with nil for NULL
		s []any
		// File content in the csv format
		csv string
	}
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"arrow keeps types and nulls", args{format: "arrow", records: []record.Record{typed, nulls}}, want{
			complete: true, keys: append([]string{"version", "complete"}, startKeys...), rows: []float64{2},
			types: map[string]string{
				"i": "int64",
				"f": "float64",
				"s": "utf8",
				"b": "bool",
				"o": "extension<arrow.json[storage_type=utf8]>",
			},
			meta: `{"version":"0.1.0",` + startMeta,
			s:    []any{"NULL", nil},
		}},
		{
			"csv marks null and writes nested values as JSON",
			args{format: "csv", records: []record.Record{
				r([]string{"n", "a", "f", "b", "i"}, nil, []any{int64(1), "x"}, 2.0, true, int64(3)),
			}},
			want{
				complete: true, keys: append([]string{"version", "complete"}, startKeys...), rows: []float64{1},
				csv: "n,a,f,b,i\n\\N,\"[1,\"\"x\"\"]\",2.0,true,3\n",
			},
		},
		{
			"a failed run keeps rows and puts the outcome before the query",
			args{format: "arrow", records: []record.Record{r([]string{"a"}, int64(1))}, runErr: errors.New("tool failed")},
			want{
				errText: "tool failed", keys: append([]string{"version", "complete", "error"}, startKeys...),
				stderr: "stopped by a failure, saved only what arrived", rows: []float64{1},
			},
		},
		{
			"a stop at --max is complete and says the budget stopped it",
			args{
				format: "arrow", records: []record.Record{typed}, max: 2.5,
				unit: "read capacity units", stop: sinks.StopBudget,
			},
			want{
				complete: true, stopped: "budget", keys: append([]string{"version", "complete", "stopped"}, budgetKeys...),
				rows: []float64{1}, types: map[string]string{
					"i": "int64",
					"f": "float64",
					"s": "utf8",
					"b": "bool",
					"o": "extension<arrow.json[storage_type=utf8]>",
				},
				meta: `{"version":"0.1.0","alias":"db","client":"mysql","user":"viewer","limit":10,` +
					`"max":2.5,"unit":"read capacity units","write":false,"follow":false,` +
					`"started":"2026-10-15T10:00:00+00:00","query":"select 1"}`,
				s: []any{"NULL"},
			},
		},
		{"an empty csv run writes one empty resource", args{format: "csv"}, want{
			complete: true, keys: append([]string{"version", "complete"}, startKeys...), rows: []float64{0}, csv: "",
		}},
		{"an empty arrow run is a readable stream", args{format: "arrow"}, want{
			complete: true, keys: append([]string{"version", "complete"}, startKeys...), rows: []float64{0},
			types: map[string]string{}, meta: `{"version":"0.1.0",` + startMeta,
		}},
		{"fields in another order or missing land in their columns", args{format: "csv", records: []record.Record{
			r([]string{"a", "b"}, "1", "2"), r([]string{"b", "a"}, "3", "4"), r([]string{"a"}, "5"),
		}}, want{
			complete: true, keys: append([]string{"version", "complete"}, startKeys...), rows: []float64{3},
			csv: "a,b\n1,2\n4,3\n5,\\N\n",
		}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			var out, errw bytes.Buffer
			m := meta
			m.Max, m.Unit = tc.args.max, tc.args.unit
			s, err := sinks.Get(dir, tc.args.format, m, &out, &errw)
			require.NoError(t, err)
			for _, rec := range tc.args.records {
				require.NoError(t, s.Write(rec))
			}
			require.NoError(t, s.Close(tc.args.stop, tc.args.runErr))

			d := readDescriptor(t, dir)
			run := d["gatereach"].(map[string]any)
			assert.Equal(t, tc.want.complete, run["complete"])
			assert.Equal(t, tc.want.errText, text(run["error"]))
			assert.Equal(t, tc.want.stopped, text(run["stopped"]))
			assert.Equal(t, tc.want.keys, gatereachKeys(t, dir))
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
				assert.Equal(t, tc.want.meta, gm)
				assert.Equal(t, tc.want.s, stringColumn(schema, batches, "s"))
			}
			if tc.args.format == "csv" {
				b, err := os.ReadFile(filepath.Join(dir, "result.csv"))
				require.NoError(t, err)
				assert.Equal(t, tc.want.csv, string(b))
			}
		})
	}
}

// Keys of the descriptor's gatereach object in the order the file holds them
func gatereachKeys(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "datapackage.json"))
	require.NoError(t, err)
	var d struct {
		Gatereach json.RawMessage `json:"gatereach"`
	}
	require.NoError(t, json.Unmarshal(b, &d))
	dec := json.NewDecoder(bytes.NewReader(d.Gatereach))
	_, err = dec.Token()
	require.NoError(t, err)
	var keys []string
	for dec.More() {
		k, err := dec.Token()
		require.NoError(t, err)
		keys = append(keys, k.(string))
		var v any
		require.NoError(t, dec.Decode(&v))
	}
	return keys
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

func TestGet_InvalidMetadata(t *testing.T) {
	type args struct {
		max  float64
		unit string
	}
	// Validation runs before any format work so one format covers both
	for _, tc := range []struct {
		name string
		args args
	}{
		{"infinite maximum", args{max: math.Inf(1), unit: "records"}},
		{"negative infinite maximum", args{max: math.Inf(-1), unit: "records"}},
		{"not a number", args{max: math.NaN(), unit: "records"}},
		{"negative maximum", args{max: -1, unit: "records"}},
		{"missing unit", args{max: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := meta
			m.Max, m.Unit = tc.args.max, tc.args.unit
			dir := filepath.Join(t.TempDir(), "out")
			s, err := sinks.Get(dir, "csv", m, &bytes.Buffer{}, &bytes.Buffer{})
			assert.Error(t, err)
			assert.Nil(t, s)
			_, statErr := os.Stat(dir)
			assert.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestGet_DescriptorReplacementFailure(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "out")
	var out bytes.Buffer
	s, err := sinks.Get(dir, "csv", meta, &out, &bytes.Buffer{})
	require.NoError(t, err)
	path := filepath.Join(dir, "datapackage.json")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	assert.Error(t, s.Close("", nil))
	temps, err := filepath.Glob(filepath.Join(dir, ".datapackage-*.json"))
	require.NoError(t, err)
	assert.Empty(t, temps)
	assert.Empty(t, out.String())
}

func TestGet_SegmentOpenFailure(t *testing.T) {
	type args struct {
		format  string
		file    string
		records []record.Record
	}
	records := []record.Record{r([]string{"a"}, "x")}
	for _, tc := range []struct {
		name string
		args args
	}{
		{"empty arrow run", args{format: "arrow", file: "result.arrows"}},
		{"empty csv run", args{format: "csv", file: "result.csv"}},
		{"buffered arrow run", args{format: "arrow", file: "result.arrows", records: records}},
		{"buffered csv run", args{format: "csv", file: "result.csv", records: records}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(t.TempDir(), "out")
			s, err := sinks.Get(dir, tc.args.format, meta, &bytes.Buffer{}, &bytes.Buffer{})
			require.NoError(t, err)
			for _, rec := range tc.args.records {
				require.NoError(t, s.Write(rec))
			}
			require.NoError(t, os.Mkdir(filepath.Join(dir, tc.args.file), 0o700))
			assert.Error(t, s.Close("", nil))
			run := readDescriptor(t, dir)["gatereach"].(map[string]any)
			assert.Equal(t, false, run["complete"])
			assert.NotEmpty(t, run["error"])
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
	require.NoError(t, s.Close("", nil))
	fi, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
}
