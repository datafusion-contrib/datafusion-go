// Adapted from cedricziel/datafusion-golang (Apache-2.0), commit 54272e7.
package native

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Expr is a filter predicate pushed down to a PushdownTableProvider. It is
// a closed, bounded AST: the engine only ever pushes column-vs-literal
// comparisons, IS [NOT] NULL, [NOT] BETWEEN, [NOT] IN, and AND/OR/NOT
// combinations of those. Predicates outside these forms (functions, casts,
// arithmetic, LIKE, ...) are never pushed down; the engine evaluates them
// after the scan.
//
// The concrete types are Compare, IsNull, Between, InList, And, Or, and
// Not. Providers pattern-match with a type switch:
//
//	switch e := expr.(type) {
//	case datafusion.Compare:
//		// e.Column, e.Op, e.Literal
//	}
type Expr interface {
	isExpr()
}

// Column identifies a column of the registered table schema, by name and
// by index into that schema. Both refer to the full registered schema,
// independent of the scan's projection.
type Column struct {
	Name  string `json:"name"`
	Index int    `json:"index"`
}

// CompareOp is a comparison operator in a pushed-down predicate.
type CompareOp string

const (
	CompareEq   CompareOp = "eq"
	CompareNeq  CompareOp = "neq"
	CompareLt   CompareOp = "lt"
	CompareLtEq CompareOp = "lteq"
	CompareGt   CompareOp = "gt"
	CompareGtEq CompareOp = "gteq"
)

// LiteralType identifies the type of a literal value in a pushed-down
// predicate.
type LiteralType string

const (
	LiteralBool      LiteralType = "bool"
	LiteralInt8      LiteralType = "int8"
	LiteralInt16     LiteralType = "int16"
	LiteralInt32     LiteralType = "int32"
	LiteralInt64     LiteralType = "int64"
	LiteralUint8     LiteralType = "uint8"
	LiteralUint16    LiteralType = "uint16"
	LiteralUint32    LiteralType = "uint32"
	LiteralUint64    LiteralType = "uint64"
	LiteralFloat32   LiteralType = "float32"
	LiteralFloat64   LiteralType = "float64"
	LiteralUtf8      LiteralType = "utf8"
	LiteralBinary    LiteralType = "binary"
	LiteralDate32    LiteralType = "date32"
	LiteralDate64    LiteralType = "date64"
	LiteralTimestamp LiteralType = "timestamp"
)

// TimeUnit is the resolution of a timestamp literal.
type TimeUnit string

const (
	TimeUnitSecond      TimeUnit = "s"
	TimeUnitMillisecond TimeUnit = "ms"
	TimeUnitMicrosecond TimeUnit = "us"
	TimeUnitNanosecond  TimeUnit = "ns"
)

// Literal is a typed literal value in a pushed-down predicate. Value holds
// the Go representation determined by Type:
//
//	LiteralBool      bool
//	LiteralInt8      int8       (int16/int32/int64 accordingly)
//	LiteralUint8     uint8      (uint16/uint32/uint64 accordingly)
//	LiteralFloat32   float32    (float64 accordingly)
//	LiteralUtf8      string
//	LiteralBinary    []byte
//	LiteralDate32    int32      (days since the Unix epoch)
//	LiteralDate64    int64      (milliseconds since the Unix epoch)
//	LiteralTimestamp int64      (in Unit since the Unix epoch)
//
// Unit and TimeZone are set only for LiteralTimestamp; TimeZone may be
// empty for a timezone-less timestamp.
type Literal struct {
	Type     LiteralType
	Value    any
	Unit     TimeUnit
	TimeZone string
}

// UnmarshalJSON decodes the wire representation of a literal, e.g.
// {"type":"int64","value":2} or
// {"type":"timestamp","value":1700000000,"unit":"s","tz":"UTC"}.
func (l *Literal) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type  LiteralType     `json:"type"`
		Value json.RawMessage `json:"value"`
		Unit  TimeUnit        `json:"unit"`
		Tz    string          `json:"tz"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("literal: %w", err)
	}
	if len(raw.Value) == 0 {
		return fmt.Errorf("literal of type %q has no value", raw.Type)
	}

	decode := func(dst any) error {
		if err := json.Unmarshal(raw.Value, dst); err != nil {
			return fmt.Errorf("literal of type %q: %w", raw.Type, err)
		}
		return nil
	}

	switch raw.Type {
	case LiteralBool:
		var v bool
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralInt8:
		var v int8
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralInt16:
		var v int16
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralInt32:
		var v int32
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralInt64:
		var v int64
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralUint8:
		var v uint8
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralUint16:
		var v uint16
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralUint32:
		var v uint32
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralUint64:
		var v uint64
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralFloat32:
		var v float32
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralFloat64:
		var v float64
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralUtf8:
		var v string
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralBinary:
		var s string
		if err := decode(&s); err != nil {
			return err
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return fmt.Errorf("literal of type %q: invalid base64: %w", raw.Type, err)
		}
		l.Value = b
	case LiteralDate32:
		var v int32
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralDate64:
		var v int64
		if err := decode(&v); err != nil {
			return err
		}
		l.Value = v
	case LiteralTimestamp:
		var v int64
		if err := decode(&v); err != nil {
			return err
		}
		switch raw.Unit {
		case TimeUnitSecond, TimeUnitMillisecond, TimeUnitMicrosecond, TimeUnitNanosecond:
		default:
			return fmt.Errorf("timestamp literal has invalid unit %q", raw.Unit)
		}
		l.Value = v
		l.Unit = raw.Unit
		l.TimeZone = raw.Tz
	default:
		return fmt.Errorf("unknown literal type %q", raw.Type)
	}
	l.Type = raw.Type
	return nil
}

// Compare is a comparison between a column and a literal. The column is
// always on the left: the engine normalizes mirrored predicates
// (2 < id becomes id > 2) before pushing them down.
type Compare struct {
	Column  Column
	Op      CompareOp
	Literal Literal
}

// IsNull is an IS NULL test on a column; Negated makes it IS NOT NULL.
type IsNull struct {
	Column  Column
	Negated bool
}

// Between is a [NOT] BETWEEN test with literal bounds (both inclusive).
type Between struct {
	Column  Column
	Negated bool
	Low     Literal
	High    Literal
}

// InList is a [NOT] IN test against a list of literals.
type InList struct {
	Column  Column
	Negated bool
	List    []Literal
}

// And is the conjunction of two predicates.
type And struct {
	Left  Expr
	Right Expr
}

// Or is the disjunction of two predicates.
type Or struct {
	Left  Expr
	Right Expr
}

// Not negates a predicate.
type Not struct {
	Expr Expr
}

func (Compare) isExpr() {}
func (IsNull) isExpr()  {}
func (Between) isExpr() {}
func (InList) isExpr()  {}
func (And) isExpr()     {}
func (Or) isExpr()      {}
func (Not) isExpr()     {}

// decodeFilters parses the engine's JSON filter document (a JSON array of
// predicate objects, combined with implicit AND) into the typed AST. Both
// serializer and parser ship in the same binary, so any decode failure is
// a bug and is surfaced loudly rather than tolerated.
func decodeFilters(data []byte) ([]Expr, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, fmt.Errorf("decoding pushed filters: %w", err)
	}
	exprs := make([]Expr, 0, len(raws))
	for i, raw := range raws {
		e, err := decodeExpr(raw)
		if err != nil {
			return nil, fmt.Errorf("decoding pushed filter %d: %w", i, err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func decodeExpr(data json.RawMessage) (Expr, error) {
	var kind struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &kind); err != nil {
		return nil, err
	}
	switch kind.Kind {
	case "compare":
		var raw struct {
			Column  Column    `json:"column"`
			Op      CompareOp `json:"op"`
			Literal Literal   `json:"literal"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		switch raw.Op {
		case CompareEq, CompareNeq, CompareLt, CompareLtEq, CompareGt, CompareGtEq:
		default:
			return nil, fmt.Errorf("unknown comparison operator %q", raw.Op)
		}
		return Compare{Column: raw.Column, Op: raw.Op, Literal: raw.Literal}, nil
	case "is_null":
		var raw struct {
			Column  Column `json:"column"`
			Negated bool   `json:"negated"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		return IsNull{Column: raw.Column, Negated: raw.Negated}, nil
	case "between":
		var raw struct {
			Column  Column  `json:"column"`
			Negated bool    `json:"negated"`
			Low     Literal `json:"low"`
			High    Literal `json:"high"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		return Between{Column: raw.Column, Negated: raw.Negated, Low: raw.Low, High: raw.High}, nil
	case "in_list":
		var raw struct {
			Column  Column    `json:"column"`
			Negated bool      `json:"negated"`
			List    []Literal `json:"list"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		return InList{Column: raw.Column, Negated: raw.Negated, List: raw.List}, nil
	case "and", "or":
		var raw struct {
			Left  json.RawMessage `json:"left"`
			Right json.RawMessage `json:"right"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		left, err := decodeExpr(raw.Left)
		if err != nil {
			return nil, err
		}
		right, err := decodeExpr(raw.Right)
		if err != nil {
			return nil, err
		}
		if kind.Kind == "and" {
			return And{Left: left, Right: right}, nil
		}
		return Or{Left: left, Right: right}, nil
	case "not":
		var raw struct {
			Expr json.RawMessage `json:"expr"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, err
		}
		inner, err := decodeExpr(raw.Expr)
		if err != nil {
			return nil, err
		}
		return Not{Expr: inner}, nil
	default:
		return nil, fmt.Errorf("unknown filter expression kind %q", kind.Kind)
	}
}
