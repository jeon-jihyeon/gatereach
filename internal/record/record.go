// Package record는 모든 소스가 내보내는 단일 레코드
package record

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// 소스 하나의 결과 한 건
// 값 타입은 아래로 한정
// 1. nil과 bool
// 2. int64와 float64
// 3. string
// 4. `map[string]any`와 `[]any`
// NULL은 nil이고 문자열 `"NULL"`과 구분
type Record struct {
	names  []string
	values []any
}

// names와 values는 같은 길이
func New(names []string, values []any) Record {
	return Record{names: names, values: values}
}

func (r Record) Names() []string { return r.names }

func (r Record) Values() []any { return r.values }

func (r Record) Len() int { return len(r.names) }

// 없는 필드는 ok false
func (r Record) Get(name string) (any, bool) {
	for i, n := range r.names {
		if n == name {
			return r.values[i], true
		}
	}
	return nil, false
}

// 필드 순서를 지킨 JSON 객체
func (r Record) JSON() []byte {
	var b bytes.Buffer
	writeObject(&b, r.names, r.values)
	return b.Bytes()
}

func ValueJSON(v any) string {
	var b bytes.Buffer
	writeValue(&b, v)
	return b.String()
}

// 폴링 중복 판별 기준
// key 필드가 있으면 그 값
// 없으면 키를 정렬한 레코드 전체
func (r Record) Identity(key string) []byte {
	if key != "" {
		if v, ok := r.Get(key); ok {
			var b bytes.Buffer
			writeValue(&b, v)
			return b.Bytes()
		}
	}
	idx := make([]int, len(r.names))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, c int) bool { return r.names[idx[a]] < r.names[idx[c]] })
	names := make([]string, len(idx))
	values := make([]any, len(idx))
	for i, j := range idx {
		names[i], values[i] = r.names[j], r.values[j]
	}
	var b bytes.Buffer
	writeObject(&b, names, values)
	return b.Bytes()
}

// 같은 이름 열은 name → name_2 → name_3 순
func UniqueNames(names []string) []string {
	seen := map[string]int{}
	out := make([]string, len(names))
	for i, n := range names {
		seen[n]++
		if seen[n] == 1 {
			out[i] = n
			continue
		}
		out[i] = fmt.Sprintf("%s_%d", n, seen[n])
	}
	return out
}

// JSON 값 하나를 레코드 값 타입으로 정규화
// 소수점이나 지수가 없는 숫자는 int64
// 그 밖의 숫자는 float64
// int64를 넘는 정수는 정밀도 보존을 위해 문자열
func Normalize(v any) any {
	switch x := v.(type) {
	case json.Number:
		s := x.String()
		if !strings.ContainsAny(s, ".eE") {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				return n
			}
			return s
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = Normalize(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = Normalize(e)
		}
		return out
	case int:
		return int64(x)
	case float32:
		return float64(x)
	}
	return v
}

// 필드 순서를 지키며 JSON 객체 하나를 레코드로 읽음
func FromJSON(data []byte) (Record, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return Record{}, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Record{}, fmt.Errorf("레코드는 JSON 객체여야 함")
	}
	var names []string
	var values []any
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return Record{}, err
		}
		var v any
		if err := dec.Decode(&v); err != nil {
			return Record{}, err
		}
		names = append(names, tok.(string))
		values = append(values, Normalize(v))
	}
	return Record{names: names, values: values}, nil
}

func writeObject(b *bytes.Buffer, names []string, values []any) {
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		writeString(b, n)
		b.WriteByte(':')
		writeValue(b, values[i])
	}
	b.WriteByte('}')
}

// 실수는 정수 값이어도 소수점을 붙여 다시 읽을 때 int64와 구분
// JSON에 없는 NaN과 무한대는 null
func writeValue(b *bytes.Buffer, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	case float64:
		b.WriteString(formatFloat(x))
	case string:
		writeString(b, x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		vals := make([]any, len(keys))
		for i, k := range keys {
			vals[i] = x[k]
		}
		writeObject(b, keys, vals)
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			writeValue(b, e)
		}
		b.WriteByte(']')
	default:
		// 허용 밖 타입은 정규화하고 그래도 모르면 텍스트로 남겨 무한 재귀 차단
		if n := Normalize(v); !sameType(n, v) {
			writeValue(b, n)
			return
		}
		writeString(b, fmt.Sprint(v))
	}
}

// 정수 값 실수에 `.0`을 붙여 다시 읽을 때 int64와 구분
// NaN과 무한대는 null
func formatFloat(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if strings.ContainsAny(s, ".eE") {
		return s
	}
	return s + ".0"
}

const hexDigits = "0123456789abcdef"

func sameType(a, b any) bool {
	return fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

// HTML 문자와 비ASCII를 그대로 둔 JSON 문자열
// 레코드마다 부르는 경로라 인코더를 만들지 않고 직접 이스케이프
// 잘못된 UTF-8은 대체 문자로 바꿈
func writeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte(hexDigits[r>>4])
			b.WriteByte(hexDigits[r&0xf])
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}
