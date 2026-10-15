package sinks

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// CSV에서 빈 문자열과 구분되는 NULL 표기
const csvNull = `\N`

// 다른 사용자가 읽지 못하게 만드는 결과 파일 권한
const fileMode = 0o600

// 스키마 하나로 이어 쓰는 데이터 파일 하나
type segment interface {
	// 한 묶음을 쓰고 바로 디스크에 반영
	write(records []record.Record) error
	close() error
	// datapackage.json의 resource 항목
	resource() ordered
	fields() []Field
}

// 형식별 구간 생성
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
}

func (b *base) fields() []Field { return b.schema }

// 스키마는 형식과 무관하게 Table Schema로 기록
// 형식별 항목
// extra는 resource에 덧붙이고 schemaExtra는 schema에 덧붙임
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

func create(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
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
	head := make([]string, len(fields))
	for i, fd := range fields {
		head[i] = fd.Name
	}
	if err := s.w.Write(head); err != nil {
		_ = f.Close()
		return nil, err
	}
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// 문자열 값 `\N`은 NULL 표기와 구분되지 않는 CSV 한계
// 구분이 필요하면 arrow 형식 사용
// object와 array 열은 JSON 텍스트
// 그 밖의 열은 문자열만 그대로 두고 나머지 값은 JSON 텍스트
func csvCell(v any, t FieldType) string {
	switch x := v.(type) {
	case nil:
		return csvNull
	case bool:
		return strconv.FormatBool(x)
	case string:
		if t == TypeObject || t == TypeArray {
			return record.ValueJSON(x)
		}
		return x
	}
	return record.ValueJSON(v)
}

func (s *csvSegment) write(records []record.Record) error {
	for _, r := range records {
		row := make([]string, len(s.schema))
		for i, f := range s.schema {
			v, _ := r.Get(f.Name)
			row[i] = csvCell(v, f.Type)
		}
		if err := s.w.Write(row); err != nil {
			return err
		}
	}
	s.w.Flush()
	s.rows += len(records)
	return s.w.Error()
}

func (s *csvSegment) close() error { return s.f.Close() }

func (s *csvSegment) resource() ordered {
	return s.base.resource("csv", "text/csv", ordered{{"encoding", "utf-8"}}, ordered{{"missingValues", []any{csvNull}}})
}

// Arrow IPC stream
// 묶음마다 record batch 하나를 이어 붙임
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
			// 형태가 일정하지 않은 값은 Arrow 정식 확장 타입 `arrow.json`으로 보존
			t = jt
		}
		afs[i] = arrow.Field{Name: f.Name, Type: t, Nullable: true}
	}
	// 파일만 따로 옮겨도 출처가 남도록 스키마 메타데이터에도 기록
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
	cols := make([]arrow.Array, len(s.schema))
	for i, f := range s.schema {
		values := make([]any, len(records))
		for j, r := range records {
			values[j], _ = r.Get(f.Name)
		}
		cols[i] = s.column(values, f.Type)
	}
	rec := array.NewRecordBatch(s.arrowSchema, cols, int64(len(records)))
	for _, c := range cols {
		c.Release()
	}
	defer rec.Release()
	if err := s.w.Write(rec); err != nil {
		return fmt.Errorf("arrow 쓰기 실패: %w", err)
	}
	s.rows += len(records)
	return nil
}

func (s *arrowSegment) close() error {
	if err := s.w.Close(); err != nil {
		return err
	}
	return s.f.Close()
}

func (s *arrowSegment) resource() ordered {
	return s.base.resource("arrows", "application/vnd.apache.arrow.stream", nil, nil)
}
