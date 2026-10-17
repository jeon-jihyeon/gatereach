package record_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

func TestJSON(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name   string
		names  []string
		values []any
		want   string
	}{
		{"keeps field order", []string{"b", "a"}, []any{int64(1), "x"}, `{"b":1,"a":"x"}`},
		{"null apart from the string NULL", []string{"a", "b"}, []any{nil, "NULL"}, `{"a":null,"b":"NULL"}`},
		{"float keeps a decimal point", []string{"f"}, []any{1.0}, `{"f":1.0}`},
		{"non-ASCII and HTML kept", []string{"s"}, []any{"café<&>"}, `{"s":"café<&>"}`},
		{"control characters escaped", []string{"s"}, []any{"a\"\\\n\r\t\x01\x1f"}, `{"s":"a\"\\\n\r\t\u0001\u001f"}`},
		{"invalid UTF-8 replaced between valid runs", []string{"s"}, []any{"é\xffa\xc3"}, `{"s":"é` + "�" + `a` + "�" + `"}`},
		{"C1 controls escaped", []string{"s"}, []any{"a\u0080b\u009f"}, `{"s":"a\u0080b\u009f"}`},
		{"line and paragraph separators escaped", []string{"s"}, []any{"a\u2028b\u2029"}, `{"s":"a\u2028b\u2029"}`},
		{"bidi controls escaped", []string{"s"}, []any{"\u202a\u202e\u2066\u2069\u200e\u200f\u061c"},
			`{"s":"\u202a\u202e\u2066\u2069\u200e\u200f\u061c"}`},
		{"runes next to the escaped ranges kept", []string{"s"}, []any{"\u00a0\u2027\u202f\u2065\u206a\u200d"},
			`{"s":"` + "\u00a0\u2027\u202f\u2065\u206a\u200d" + `"}`},
		{"NaN is null", []string{"f"}, []any{math.NaN()}, `{"f":null}`},
		{"nested map sorted", []string{"m"}, []any{map[string]any{"z": int64(1), "a": []any{true, nil}}},
			`{"m":{"a":[true,null],"z":1}}`},
		{"int normalized", []string{"n"}, []any{7}, `{"n":7}`},
		{"unknown type written as text", []string{"t"}, []any{time.Duration(1)}, `{"t":"1ns"}`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, string(record.New(tc.names, tc.values).JSON()))
		})
	}
}

func TestFromJSON(t *testing.T) {
	t.Parallel()
	type want struct {
		names  []string
		values []any
		err    bool
	}
	tcs := []struct {
		name string
		in   string
		want want
	}{
		{"round trip keeps order and types", `{"z":"x","a":{"k":2.5},"f":3.0,"i":9007199254740993,"n":null}`,
			want{
				names:  []string{"z", "a", "f", "i", "n"},
				values: []any{"x", map[string]any{"k": 2.5}, 3.0, int64(9007199254740993), nil},
			}},
		{"integer past int64 stays a string", `{"n":123456789012345678901234567890}`,
			want{names: []string{"n"}, values: []any{"123456789012345678901234567890"}}},
		{"number past float64 stays a string", `{"n":1e400}`, want{names: []string{"n"}, values: []any{"1e400"}}},
		{"not an object", `[1]`, want{err: true}},
		{"broken JSON", `{"a":`, want{err: true}},
		{"object without its closing brace", `{"a":1`, want{err: true}},
		{"data after the object", `{"a":1}{"b":2}`, want{err: true}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := record.FromJSON([]byte(tc.in))
			assert.Equal(t, tc.want.err, err != nil, "%v", err)
			assert.Equal(t, tc.want.names, got.Names())
			assert.Equal(t, tc.want.values, got.Values())
		})
	}
}

func TestIdentity(t *testing.T) {
	t.Parallel()
	a := record.New([]string{"id", "v"}, []any{int64(1), "x"})
	b := record.New([]string{"v", "id"}, []any{"x", int64(1)})
	tcs := []struct {
		name string
		got  []byte
		want []byte
	}{
		{"whole record ignores field order", a.Identity(""), b.Identity("")},
		{"key picks the field", a.Identity("id"), []byte("1")},
		{"missing key falls back to the whole record", a.Identity("missing"), a.Identity("")},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestUniqueNames(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		in   []string
		want []string
	}{
		{"suffixes repeats", []string{"id", "id", "name", "id"}, []string{"id", "id_2", "name", "id_3"}},
		{"keeps distinct names", []string{"a", "b"}, []string{"a", "b"}},
		{"a suffix skips a name already in the list", []string{"id", "id", "id_2"}, []string{"id", "id_3", "id_2"}},
		{"a suffixed repeat gets its own suffix", []string{"a", "a_2", "a", "a_2"}, []string{"a", "a_2", "a_3", "a_2_2"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, record.UniqueNames(tc.in))
		})
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	r := record.New([]string{"a"}, []any{"x"})
	type want struct {
		value any
		ok    bool
	}
	tcs := []struct {
		name  string
		field string
		want  want
	}{
		{"present", "a", want{"x", true}},
		{"absent", "b", want{nil, false}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, ok := r.Get(tc.field)
			assert.Equal(t, tc.want, want{v, ok})
		})
	}
}
