// Package record is the one record shape every source emits
package record

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// One result row of a source
// Values are limited to these types
// 1. nil and bool
// 2. int64 and float64
// 3. string
// 4. `map[string]any` and `[]any`
// NULL is nil and stays apart from the string `"NULL"`
type Record struct {
	names  []string
	values []any
}

// The caller keeps names and values the same length
func New(names []string, values []any) Record {
	return Record{names: names, values: values}
}

func (r Record) Names() []string { return r.names }

func (r Record) Values() []any { return r.values }

func (r Record) Len() int { return len(r.names) }

func (r Record) Get(name string) (any, bool) {
	for i, n := range r.names {
		if n == name {
			return r.values[i], true
		}
	}
	return nil, false
}

// A JSON object in field order
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

// What tells polled records apart
// 1. The key field's value when the record has it
// 2. Otherwise the whole record with its keys sorted
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

// Repeated column names become name → name_2 → name_3
// A suffix skips every name already in the list so `id id id_2` gives `id id_3 id_2`
func UniqueNames(names []string) []string {
	taken := make(map[string]bool, len(names))
	for _, n := range names {
		taken[n] = true
	}
	first := make(map[string]bool, len(names))
	next := map[string]int{}
	out := make([]string, len(names))
	for i, n := range names {
		if !first[n] {
			first[n] = true
			out[i] = n
			continue
		}
		k := max(next[n], 2)
		for taken[fmt.Sprintf("%s_%d", n, k)] {
			k++
		}
		out[i] = fmt.Sprintf("%s_%d", n, k)
		taken[out[i]] = true
		next[n] = k + 1
	}
	return out
}

// Maps a JSON value onto the record value types
// 1. A number without a decimal point or exponent is int64
// 2. Any other number is float64
// 3. An integer past int64 or a number past float64 is a string to keep its value
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
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
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

// Keeps the field order of the JSON object
func FromJSON(data []byte) (Record, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return Record{}, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Record{}, errors.New("a record must be a JSON object")
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
	// More stops at the end of the input too so a cut object needs its closing brace checked
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return Record{}, errors.New("a record must be one whole JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Record{}, errors.New("a record must be one whole JSON object")
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

// Map keys are sorted so the same value always gives the same bytes
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
		// Other types are normalized first and anything still unknown becomes text
		// so this never recurses forever
		if n := Normalize(v); reflect.TypeOf(n) != reflect.TypeOf(v) {
			writeValue(b, n)
			return
		}
		writeString(b, fmt.Sprint(v))
	}
}

// 1. An integer-valued float gets `.0` so it reads back apart from int64
// 2. NaN and infinity have no JSON form and become null
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

// Runes escaped so a value can't hide text or reorder what a terminal or a reader shows
// 1. C1 controls U+0080 to U+009F
// 2. Line and paragraph separators U+2028 and U+2029 that end a line in JavaScript
// 3. Bidi marks U+200E U+200F and U+061C and the embeddings and isolates U+202A to U+202E and U+2066 to U+2069
func hiddenRune(r rune) bool {
	switch {
	case r >= 0x80 && r <= 0x9f, r == 0x2028, r == 0x2029, r == 0x200e, r == 0x200f, r == 0x061c:
		return true
	case r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069:
		return true
	}
	return false
}

// A JSON string that leaves HTML characters and non-ASCII as is apart from hiddenRune
// It runs for every string of every record
// 1. It escapes by hand instead of building an encoder
// 2. Runs of bytes that need no escape are copied in one write
// 3. Invalid UTF-8 becomes the replacement character
func writeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[i:])
			switch {
			case r == utf8.RuneError && size == 1:
				b.WriteString(s[start:i])
				b.WriteRune(utf8.RuneError)
				start = i + size
			case hiddenRune(r):
				b.WriteString(s[start:i])
				b.WriteString(`\u`)
				for shift := 12; shift >= 0; shift -= 4 {
					b.WriteByte(hexDigits[r>>shift&0xf])
				}
				start = i + size
			}
			i += size
			continue
		}
		if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}
		b.WriteString(s[start:i])
		switch c {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteString(`\u00`)
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
		}
		i++
		start = i
	}
	b.WriteString(s[start:])
	b.WriteByte('"')
}
