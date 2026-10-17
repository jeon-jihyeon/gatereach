package sinks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

func values(vs ...any) []record.Record {
	out := make([]record.Record, len(vs))
	for i, v := range vs {
		out[i] = record.New([]string{"v"}, []any{v})
	}
	return out
}

func TestInfer(t *testing.T) {
	t.Parallel()
	big := int64(1) << 60
	tcs := []struct {
		name    string
		records []record.Record
		fields  []Field
		want    FieldType
	}{
		{"integers", values(int64(1), big), nil, TypeInteger},
		{"a lone big integer", values(big), nil, TypeInteger},
		{"strings and nulls", values("x", nil), nil, TypeString},
		{"booleans", values(true), nil, TypeBoolean},
		{"objects", values(map[string]any{}), nil, TypeObject},
		{"arrays", values([]any{}), nil, TypeArray},
		{"integer and float", values(int64(1), 0.5), nil, TypeNumber},
		{"big integer and float stay exact", values(0.5, big), nil, TypeAny},
		{"only nulls", values(nil), nil, TypeAny},
		{"string and number", values("x", 0.5), nil, TypeAny},
		{"an existing any stays any", values("x"), []Field{{"v", TypeAny}}, TypeAny},
		{"an existing integer widens with a float", values(0.5), []Field{{"v", TypeInteger}}, TypeNumber},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := infer(tc.records, tc.fields)
			require.Len(t, got, 1)
			assert.Equal(t, tc.want, got[0].Type)
		})
	}
}

func TestFits(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name    string
		records []record.Record
		fields  []Field
		want    bool
	}{
		{"a large integer never lands in a float column", values(int64(1) << 60), []Field{{"v", TypeNumber}}, false},
		{"a small integer fits a float column", values(int64(3)), []Field{{"v", TypeNumber}}, true},
		{"null fits any column", values(nil), []Field{{"v", TypeBoolean}}, true},
		{"a new field does not fit", []record.Record{record.New([]string{"w"}, []any{"x"})}, []Field{{"v", TypeString}},
			false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fits(tc.records, tc.fields))
		})
	}
}
