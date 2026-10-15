package sinks

import (
	"github.com/jeon-jihyeon/gatereach/internal/record"
)

// float64가 정확히 표현하는 정수 범위
const floatSafe = 1 << 53

// Table Schema 필드 타입
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeInteger FieldType = "integer"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeAny     FieldType = "any" // 종류가 섞인 열
)

// Table Schema 필드 하나
type Field struct {
	Name string
	Type FieldType
}

type valueKind string

const (
	kindNull    valueKind = ""
	kindString  valueKind = "string"
	kindInteger valueKind = "integer" // float64로 옮겨도 정확한 정수
	kindBigint  valueKind = "bigint"  // int64 안이지만 float64로 옮기면 정밀도를 잃는 정수
	kindNumber  valueKind = "number"
	kindBoolean valueKind = "boolean"
	kindObject  valueKind = "object"
	kindArray   valueKind = "array"
)

// 필드 타입별로 받아들이는 값 종류
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

// 기존 필드 타입이 받는 값 종류
func kindsOf(t FieldType) []valueKind {
	switch t {
	case TypeInteger:
		return []valueKind{kindInteger}
	case TypeAny:
		return []valueKind{kindString, kindNumber}
	}
	return []valueKind{valueKind(t)}
}

// 관측한 값 종류로만 결정해 값 추측 없음
func merge(kinds map[valueKind]bool) FieldType {
	delete(kinds, kindNull)
	if len(kinds) == 0 {
		return TypeAny
	}
	only := func(allowed ...valueKind) bool {
		set := map[valueKind]bool{}
		for _, a := range allowed {
			set[a] = true
		}
		for k := range kinds {
			if !set[k] {
				return false
			}
		}
		return true
	}
	switch {
	case only(kindInteger, kindBigint):
		return TypeInteger
	case len(kinds) == 1:
		for k := range kinds {
			return FieldType(k)
		}
	case only(kindInteger, kindNumber):
		return TypeNumber
	}
	return TypeAny
}

// 기존 필드 순서와 타입을 유지하며 레코드들의 필드와 타입을 합침
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

// 현재 구간 스키마로 손실 없이 쓸 수 있는지
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
