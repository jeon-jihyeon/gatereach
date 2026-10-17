package sinks

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// The CSV NULL marker that stays apart from an empty string
const csvNull = `\N`

// Keeps other users from reading result files
const fileMode = 0o600

// One data file appended under one schema
type segment interface {
	// Writes one batch and flushes it to the file right away
	write(records []record.Record) error
	close() error
	// The resource entry in datapackage.json
	resource() ordered
	fields() []Field
}

type format struct {
	suffix string
	open   func(name, path string, fields []Field, meta []byte) (segment, error)
}

var Formats = map[string]format{
	"arrow": {suffix: "arrows", open: openArrow},
	"csv":   {suffix: "csv", open: openCSV},
}

type base struct {
	name   string
	path   string
	schema []Field
	rows   int
	// Field names of the last record looked up and where each schema field sits in them
	lastNames []string
	positions []int
}

func (b *base) fields() []Field { return b.schema }

// Position of each schema field in the record's names or -1 when the record lacks it
// Rows of one result share their names so the last answer is reused
// That keeps a wide row linear instead of one name scan per field
func (b *base) positionsOf(names []string) []int {
	if b.positions != nil && slices.Equal(b.lastNames, names) {
		return b.positions
	}
	at := make(map[string]int, len(names))
	for i, n := range names {
		// The first of repeated names wins like the record's own lookup
		if _, ok := at[n]; !ok {
			at[n] = i
		}
	}
	pos := make([]int, len(b.schema))
	for i, f := range b.schema {
		pos[i] = -1
		if j, ok := at[f.Name]; ok {
			pos[i] = j
		}
	}
	b.lastNames, b.positions = names, pos
	return pos
}

// The schema is a Table Schema whatever the format
// Format-specific entries go in extra for the resource and schemaExtra for the schema
func (b *base) resource(format, mediatype string, extra, schemaExtra ordered) ordered {
	fs := make([]any, len(b.schema))
	for i, f := range b.schema {
		fs[i] = ordered{{"name", f.Name}, {"type", f.Type}}
	}
	schema := append(ordered{{"fields", fs}}, schemaExtra...)
	r := ordered{{"name", b.name}, {"path", filepath.Base(b.path)}, {"type", "table"}, {"format", format}, {"mediatype", mediatype}}
	r = append(r, extra...)
	return append(r, entry{"schema", schema}, entry{"gatereach", ordered{{"rows", b.rows}}})
}

// Never opens a file or link that is already there
// So a link planted in the folder can't redirect a write
func create(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, fileMode)
}

type csvSegment struct {
	base
	f *os.File
	w *csv.Writer
}

func openCSV(name, path string, fields []Field, _ []byte) (segment, error) {
	f, err := create(path)
	if err != nil {
		return nil, err
	}
	s := &csvSegment{base: base{name: name, path: path, schema: fields}, f: f, w: csv.NewWriter(f)}
	// A header of no fields would be a lone newline that CSV readers such as pandas reject
	// An empty file reads as no data instead
	if len(fields) == 0 {
		return s, nil
	}
	head := make([]string, len(fields))
	for i, fd := range fields {
		head[i] = fd.Name
	}
	if err := s.w.Write(head); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("csv write failed: %w", err)
	}
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("csv write failed: %w", err)
	}
	return s, nil
}

// 1. NULL is `\N` so a string `\N` reads back as NULL
// 2. A string stays as is
// 3. Every other value is JSON text so an object or an array cell holds JSON
// The arrow format keeps NULL apart from every string so it's the default
func csvCell(v any) string {
	switch x := v.(type) {
	case nil:
		return csvNull
	case string:
		return x
	}
	return record.ValueJSON(v)
}

func (s *csvSegment) write(records []record.Record) error {
	// The csv writer copies each row out so one row slice serves the batch
	row := make([]string, len(s.schema))
	for _, r := range records {
		values := r.Values()
		for i, p := range s.positionsOf(r.Names()) {
			var v any
			if p >= 0 {
				v = values[p]
			}
			row[i] = csvCell(v)
		}
		if err := s.w.Write(row); err != nil {
			return fmt.Errorf("csv write failed: %w", err)
		}
	}
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		return fmt.Errorf("csv write failed: %w", err)
	}
	s.rows += len(records)
	return nil
}

func (s *csvSegment) close() error { return s.f.Close() }

func (s *csvSegment) resource() ordered {
	return s.base.resource("csv", "text/csv", ordered{{"encoding", "utf-8"}}, ordered{{"missingValues", []any{csvNull}}})
}

// Arrow IPC stream
// Each write appends one Arrow record batch
type arrowSegment struct {
	base
	f           *os.File
	w           *ipc.Writer
	arrowSchema *arrow.Schema
	json        *extensions.JSONType
}

func openArrow(name, path string, fields []Field, meta []byte) (segment, error) {
	jt, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		return nil, err
	}
	afs := make([]arrow.Field, len(fields))
	for i, f := range fields {
		var t arrow.DataType
		switch f.Type {
		case TypeString:
			t = arrow.BinaryTypes.String
		case TypeInteger:
			t = arrow.PrimitiveTypes.Int64
		case TypeNumber:
			t = arrow.PrimitiveTypes.Float64
		case TypeBoolean:
			t = arrow.FixedWidthTypes.Boolean
		default:
			// Values without a fixed shape are kept as the canonical Arrow extension type `arrow.json`
			t = jt
		}
		afs[i] = arrow.Field{Name: f.Name, Type: t, Nullable: true}
	}
	// Also written to the schema metadata so the file keeps its origin even when moved alone
	md := arrow.NewMetadata([]string{"gatereach"}, []string{string(meta)})
	schema := arrow.NewSchema(afs, &md)
	f, err := create(path)
	if err != nil {
		return nil, err
	}
	return &arrowSegment{
		base:        base{name: name, path: path, schema: fields},
		f:           f,
		w:           ipc.NewWriter(f, ipc.WithSchema(schema)),
		arrowSchema: schema,
		json:        jt,
	}, nil
}

func (s *arrowSegment) column(values []any, t FieldType) arrow.Array {
	mem := memory.DefaultAllocator
	switch t {
	case TypeString:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(x)
			default:
				// A value outside the record types counts as a string and is kept as its JSON text
				b.Append(record.ValueJSON(x))
			}
		}
		return b.NewArray()
	case TypeInteger:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, v := range values {
			if x, ok := v.(int64); ok {
				b.Append(x)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	case TypeNumber:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case float64:
				b.Append(x)
			case int64:
				b.Append(float64(x))
			default:
				b.AppendNull()
			}
		}
		return b.NewArray()
	case TypeBoolean:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, v := range values {
			if x, ok := v.(bool); ok {
				b.Append(x)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	}
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for _, v := range values {
		if v == nil {
			b.AppendNull()
		} else {
			b.Append(record.ValueJSON(v))
		}
	}
	storage := b.NewArray()
	defer storage.Release()
	return array.NewExtensionArrayWithStorage(s.json, storage)
}

func (s *arrowSegment) write(records []record.Record) error {
	columns := make([][]any, len(s.schema))
	for i := range columns {
		columns[i] = make([]any, len(records))
	}
	for j, r := range records {
		values := r.Values()
		for i, p := range s.positionsOf(r.Names()) {
			if p >= 0 {
				columns[i][j] = values[p]
			}
		}
	}
	cols := make([]arrow.Array, len(s.schema))
	for i, f := range s.schema {
		cols[i] = s.column(columns[i], f.Type)
	}
	rec := array.NewRecordBatch(s.arrowSchema, cols, int64(len(records)))
	for _, c := range cols {
		c.Release()
	}
	defer rec.Release()
	if err := s.w.Write(rec); err != nil {
		return fmt.Errorf("arrow write failed: %w", err)
	}
	s.rows += len(records)
	return nil
}

// The file closes even when the stream's end marker fails to write
func (s *arrowSegment) close() error {
	werr := s.w.Close()
	ferr := s.f.Close()
	if werr != nil {
		return fmt.Errorf("arrow write failed: %w", werr)
	}
	return ferr
}

func (s *arrowSegment) resource() ordered {
	return s.base.resource("arrows", "application/vnd.apache.arrow.stream", nil, nil)
}
