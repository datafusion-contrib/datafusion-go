// Adapted from cedricziel/datafusion-golang (Apache-2.0), commit 54272e7.
package native

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "pushdown", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return data
}

func TestDecodeFilters_CompareGtInt64(t *testing.T) {
	exprs, err := decodeFilters(readFixture(t, "compare_gt_int64.json"))
	if err != nil {
		t.Fatalf("decodeFilters: %v", err)
	}
	want := []Expr{
		Compare{
			Column:  Column{Name: "id", Index: 0},
			Op:      CompareGt,
			Literal: Literal{Type: LiteralInt64, Value: int64(2)},
		},
	}
	if !reflect.DeepEqual(exprs, want) {
		t.Fatalf("got %#v, want %#v", exprs, want)
	}
}

func TestDecodeFilters_AllLiteralTypes(t *testing.T) {
	exprs, err := decodeFilters(readFixture(t, "literal_types.json"))
	if err != nil {
		t.Fatalf("decodeFilters: %v", err)
	}
	want := []Expr{
		Compare{Column: Column{Name: "c_bool", Index: 0}, Op: CompareEq,
			Literal: Literal{Type: LiteralBool, Value: true}},
		Compare{Column: Column{Name: "c_i8", Index: 1}, Op: CompareNeq,
			Literal: Literal{Type: LiteralInt8, Value: int8(-8)}},
		Compare{Column: Column{Name: "c_i16", Index: 2}, Op: CompareLt,
			Literal: Literal{Type: LiteralInt16, Value: int16(-1600)}},
		Compare{Column: Column{Name: "c_i32", Index: 3}, Op: CompareLtEq,
			Literal: Literal{Type: LiteralInt32, Value: int32(-320000)}},
		Compare{Column: Column{Name: "c_i64", Index: 4}, Op: CompareGt,
			Literal: Literal{Type: LiteralInt64, Value: int64(9223372036854775807)}},
		Compare{Column: Column{Name: "c_u8", Index: 5}, Op: CompareGtEq,
			Literal: Literal{Type: LiteralUint8, Value: uint8(255)}},
		Compare{Column: Column{Name: "c_u16", Index: 6}, Op: CompareEq,
			Literal: Literal{Type: LiteralUint16, Value: uint16(65535)}},
		Compare{Column: Column{Name: "c_u32", Index: 7}, Op: CompareEq,
			Literal: Literal{Type: LiteralUint32, Value: uint32(4294967295)}},
		Compare{Column: Column{Name: "c_u64", Index: 8}, Op: CompareEq,
			Literal: Literal{Type: LiteralUint64, Value: uint64(18446744073709551615)}},
		Compare{Column: Column{Name: "c_f32", Index: 9}, Op: CompareGt,
			Literal: Literal{Type: LiteralFloat32, Value: float32(3.5)}},
		Compare{Column: Column{Name: "c_f64", Index: 10}, Op: CompareLt,
			Literal: Literal{Type: LiteralFloat64, Value: float64(2.25)}},
		Compare{Column: Column{Name: "c_str", Index: 11}, Op: CompareEq,
			Literal: Literal{Type: LiteralUtf8, Value: `hello "world"`}},
		Compare{Column: Column{Name: "c_bin", Index: 12}, Op: CompareEq,
			Literal: Literal{Type: LiteralBinary, Value: []byte{0xde, 0xad, 0xbe, 0xef}}},
		Compare{Column: Column{Name: "c_d32", Index: 13}, Op: CompareGtEq,
			Literal: Literal{Type: LiteralDate32, Value: int32(19000)}},
		Compare{Column: Column{Name: "c_d64", Index: 14}, Op: CompareLtEq,
			Literal: Literal{Type: LiteralDate64, Value: int64(1700000000000)}},
		Compare{Column: Column{Name: "c_ts_s", Index: 15}, Op: CompareGt,
			Literal: Literal{Type: LiteralTimestamp, Value: int64(1700000000), Unit: TimeUnitSecond}},
		Compare{Column: Column{Name: "c_ts_ms", Index: 16}, Op: CompareLt,
			Literal: Literal{Type: LiteralTimestamp, Value: int64(1700000000123), Unit: TimeUnitMillisecond}},
		Compare{Column: Column{Name: "c_ts_us", Index: 17}, Op: CompareNeq,
			Literal: Literal{Type: LiteralTimestamp, Value: int64(1700000000123456), Unit: TimeUnitMicrosecond}},
		Compare{Column: Column{Name: "c_ts_ns", Index: 18}, Op: CompareEq,
			Literal: Literal{Type: LiteralTimestamp, Value: int64(1700000000123456789), Unit: TimeUnitNanosecond, TimeZone: "UTC"}},
	}
	if len(exprs) != len(want) {
		t.Fatalf("got %d exprs, want %d", len(exprs), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(exprs[i], want[i]) {
			t.Errorf("expr %d:\n got %#v\nwant %#v", i, exprs[i], want[i])
		}
	}
}

func TestDecodeFilters_Composite(t *testing.T) {
	exprs, err := decodeFilters(readFixture(t, "composite.json"))
	if err != nil {
		t.Fatalf("decodeFilters: %v", err)
	}
	id := Column{Name: "id", Index: 0}
	name := Column{Name: "name", Index: 1}
	want := []Expr{
		And{
			Left: Compare{Column: id, Op: CompareGtEq, Literal: Literal{Type: LiteralInt64, Value: int64(1)}},
			Right: Or{
				Left: IsNull{Column: name},
				Right: Not{Expr: Compare{Column: name, Op: CompareEq,
					Literal: Literal{Type: LiteralUtf8, Value: "x"}}},
			},
		},
		Between{
			Column: id, Negated: true,
			Low:  Literal{Type: LiteralInt64, Value: int64(10)},
			High: Literal{Type: LiteralInt64, Value: int64(20)},
		},
		InList{
			Column: name,
			List: []Literal{
				{Type: LiteralUtf8, Value: "a"},
				{Type: LiteralUtf8, Value: "b"},
			},
		},
		IsNull{Column: name, Negated: true},
	}
	if !reflect.DeepEqual(exprs, want) {
		t.Fatalf("got %#v\nwant %#v", exprs, want)
	}
}

func TestDecodeFilters_Errors(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		wantSub string
	}{
		{"not json", `{`, "decoding pushed filters"},
		{"not an array", `{"kind":"compare"}`, "decoding pushed filters"},
		{"unknown kind", `[{"kind":"like"}]`, "unknown filter expression kind"},
		{"missing kind", `[{"op":"eq"}]`, "unknown filter expression kind"},
		{"unknown op", `[{"kind":"compare","column":{"name":"id","index":0},"op":"~","literal":{"type":"int64","value":1}}]`, "unknown comparison operator"},
		{"unknown literal type", `[{"kind":"compare","column":{"name":"id","index":0},"op":"eq","literal":{"type":"decimal128","value":"1.5"}}]`, "unknown literal type"},
		{"bad literal value", `[{"kind":"compare","column":{"name":"id","index":0},"op":"eq","literal":{"type":"int64","value":"nope"}}]`, "literal"},
		{"int overflow", `[{"kind":"compare","column":{"name":"c","index":0},"op":"eq","literal":{"type":"int8","value":1000}}]`, "literal"},
		{"bad base64", `[{"kind":"compare","column":{"name":"c","index":0},"op":"eq","literal":{"type":"binary","value":"!!"}}]`, "literal"},
		{"missing timestamp unit", `[{"kind":"compare","column":{"name":"c","index":0},"op":"eq","literal":{"type":"timestamp","value":1}}]`, "timestamp"},
		{"bad nested expr", `[{"kind":"and","left":{"kind":"nope"},"right":{"kind":"nope"}}]`, "unknown filter expression kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeFilters([]byte(tc.doc))
			if err == nil {
				t.Fatalf("expected error for %s", tc.doc)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err, tc.wantSub)
			}
		})
	}
}

func TestDecodeFilters_EmptyArray(t *testing.T) {
	exprs, err := decodeFilters(bytes.TrimSpace([]byte("[]")))
	if err != nil {
		t.Fatalf("decodeFilters: %v", err)
	}
	if len(exprs) != 0 {
		t.Fatalf("expected no exprs, got %#v", exprs)
	}
}
