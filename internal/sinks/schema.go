package sinks

import (
	"slices"

	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// Integers up to this size are exact in float64
const floatSafe = 1 << 53

// Table Schema field type
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeInteger FieldType = "integer"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeAny     FieldType = "any" // mixed kinds or only nulls
)

type Field struct {
	Name string
	Type FieldType
}

type valueKind string

const (
	kindNull    valueKind = ""
	kindString  valueKind = "string"
	kindInteger valueKind = "integer" // an integer that stays exact in float64
	kindBigint  valueKind = "bigint"  // an int64 that loses precision in float64
	kindNumber  valueKind = "number"
	kindBoolean valueKind = "boolean"
	kindObject  valueKind = "object"
	kindArray   valueKind = "array"
)

// Value kinds each field type accepts
var accepts = map[FieldType]map[valueKind]bool{
	TypeString:  {kindString: true},
	TypeInteger: {kindInteger: true, kindBigint: true},
	TypeNumber:  {kindInteger: true, kindNumber: true},
	TypeBoolean: {kindBoolean: true},
	TypeObject:  {kindObject: true},
	TypeArray:   {kindArray: true},
	TypeAny:     {kindString: true, kindInteger: true, kindBigint: true, kindNumber: true, kindBoolean: true, kindObject: true, kindArray: true},
}

func kindOf(v any) valueKind {
	switch x := v.(type) {
	case nil:
		return kindNull
	case bool:
		return kindBoolean
	case int64:
		if x >= -floatSafe && x <= floatSafe {
			return kindInteger
		}
		return kindBigint
	case float64:
		return kindNumber
	case map[string]any:
		return kindObject
	case []any:
		return kindArray
	}
	return kindString
}

// Value kinds an existing field type stands for when it merges with new values
// 1. integer stands for small integers so a float widens it to number
// 2. any stands for two unrelated kinds so it always merges back to any
func kindsOf(t FieldType) []valueKind {
	switch t {
	case TypeInteger:
		return []valueKind{kindInteger}
	case TypeAny:
		return []valueKind{kindString, kindNumber}
	}
	return []valueKind{valueKind(t)}
}

// Decides only from the kinds seen and never guesses from values
func merge(kinds map[valueKind]bool) FieldType {
	delete(kinds, kindNull)
	if len(kinds) == 0 {
		return TypeAny
	}
	switch {
	case onlyKinds(kinds, kindInteger, kindBigint):
		return TypeInteger
	case len(kinds) == 1:
		// Every other kind is named after the field type that holds it
		for k := range kinds {
			return FieldType(k)
		}
	case onlyKinds(kinds, kindInteger, kindNumber):
		return TypeNumber
	}
	return TypeAny
}

func onlyKinds(kinds map[valueKind]bool, allowed ...valueKind) bool {
	for k := range kinds {
		if !slices.Contains(allowed, k) {
			return false
		}
	}
	return true
}

// The schema that holds both fields and the records
// 1. Existing fields keep their order and new ones follow in the order they appear
// 2. A type only widens
func infer(records []record.Record, fields []Field) []Field {
	var order []string
	seen := map[string]map[valueKind]bool{}
	add := func(name string, ks ...valueKind) {
		if seen[name] == nil {
			seen[name] = map[valueKind]bool{}
			order = append(order, name)
		}
		for _, k := range ks {
			seen[name][k] = true
		}
	}
	for _, f := range fields {
		add(f.Name, kindsOf(f.Type)...)
	}
	for _, r := range records {
		for i, n := range r.Names() {
			add(n, kindOf(r.Values()[i]))
		}
	}
	out := make([]Field, len(order))
	for i, n := range order {
		out[i] = Field{Name: n, Type: merge(seen[n])}
	}
	return out
}

// Whether the records fit the current segment's schema without loss
func fits(records []record.Record, fields []Field) bool {
	types := map[string]FieldType{}
	for _, f := range fields {
		types[f.Name] = f.Type
	}
	for _, r := range records {
		for i, n := range r.Names() {
			t, ok := types[n]
			if !ok {
				return false
			}
			k := kindOf(r.Values()[i])
			if k != kindNull && !accepts[t][k] {
				return false
			}
		}
	}
	return true
}
