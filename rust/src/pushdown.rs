// Adapted from cedricziel/datafusion-golang (Apache-2.0), commit 54272e7.
//! Recognizer and serializer for the bounded filter-pushdown predicate AST
//! (design D2/D3/D7). One recognizer, [`classify`], decides which
//! DataFusion expressions are representable; it backs both planning-time
//! classification (`supports_filters_pushdown`) and scan-time
//! serialization, so serialization is total by construction.

use std::ffi::CString;

use base64::Engine as _;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct ColumnRef {
    name: String,
    index: usize,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CompareOp {
    Eq,
    Neq,
    Lt,
    Lteq,
    Gt,
    Gteq,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum Literal {
    Bool {
        value: bool,
    },
    Int8 {
        value: i8,
    },
    Int16 {
        value: i16,
    },
    Int32 {
        value: i32,
    },
    Int64 {
        value: i64,
    },
    Uint8 {
        value: u8,
    },
    Uint16 {
        value: u16,
    },
    Uint32 {
        value: u32,
    },
    Uint64 {
        value: u64,
    },
    Float32 {
        value: f32,
    },
    Float64 {
        value: f64,
    },
    Utf8 {
        value: String,
    },
    /// Binary literals cross the wire base64-encoded (standard alphabet).
    Binary {
        value: String,
    },
    Date32 {
        value: i32,
    },
    Date64 {
        value: i64,
    },
    Timestamp {
        value: i64,
        unit: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        tz: Option<String>,
    },
}

/// One node of the wire-format predicate AST, tagged by `kind`. The Go
/// decoder in `datafusion/expr.go` is the other half of this contract;
/// golden fixtures under `datafusion/testdata/pushdown/` pin both sides.
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PushExpr {
    Compare {
        column: ColumnRef,
        op: CompareOp,
        literal: Literal,
    },
    IsNull {
        column: ColumnRef,
        negated: bool,
    },
    Between {
        column: ColumnRef,
        negated: bool,
        low: Literal,
        high: Literal,
    },
    InList {
        column: ColumnRef,
        negated: bool,
        list: Vec<Literal>,
    },
    And {
        left: Box<PushExpr>,
        right: Box<PushExpr>,
    },
    Or {
        left: Box<PushExpr>,
        right: Box<PushExpr>,
    },
    Not {
        expr: Box<PushExpr>,
    },
}

/// Decides whether `expr` is representable in the bounded AST, returning
/// the wire node if so. Conservative by design (design D7): anything not
/// recognized is `None`, which the caller maps to `Unsupported` — always
/// correct, since DataFusion then filters after the scan.
pub(crate) fn classify(expr: &Expr, schema: &Schema) -> Option<PushExpr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Some(PushExpr::And {
                left: Box::new(classify(left, schema)?),
                right: Box::new(classify(right, schema)?),
            }),
            Operator::Or => Some(PushExpr::Or {
                left: Box::new(classify(left, schema)?),
                right: Box::new(classify(right, schema)?),
            }),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                if let (Some(column), Some(literal)) = (column_ref(left, schema), literal(right)) {
                    Some(PushExpr::Compare {
                        column,
                        op: compare_op(*op)?,
                        literal,
                    })
                } else if let (Some(literal), Some(column)) =
                    (literal(left), column_ref(right, schema))
                {
                    // Normalize `lit op col` so Go always sees the column
                    // on the left.
                    Some(PushExpr::Compare {
                        column,
                        op: compare_op(op.swap()?)?,
                        literal,
                    })
                } else {
                    None
                }
            }
            _ => None,
        },
        Expr::IsNull(inner) => Some(PushExpr::IsNull {
            column: column_ref(inner, schema)?,
            negated: false,
        }),
        Expr::IsNotNull(inner) => Some(PushExpr::IsNull {
            column: column_ref(inner, schema)?,
            negated: true,
        }),
        Expr::Not(inner) => Some(PushExpr::Not {
            expr: Box::new(classify(inner, schema)?),
        }),
        Expr::Between(between) => Some(PushExpr::Between {
            column: column_ref(&between.expr, schema)?,
            negated: between.negated,
            low: literal(&between.low)?,
            high: literal(&between.high)?,
        }),
        Expr::InList(in_list) => Some(PushExpr::InList {
            column: column_ref(&in_list.expr, schema)?,
            negated: in_list.negated,
            list: in_list
                .list
                .iter()
                .map(literal)
                .collect::<Option<Vec<_>>>()?,
        }),
        _ => None,
    }
}

/// Serializes the representable subset of `filters` into the one-JSON-
/// document-per-scan wire format (design D3). Returns `None` when nothing
/// is representable (the scan then passes NULL). Filters reaching a scan
/// were already classified representable at planning time (design D7);
/// filtering again here is defensive and always correct, since pushed
/// filters are advisory.
pub(crate) fn filters_to_json(filters: &[Expr], schema: &Schema) -> Option<CString> {
    let nodes: Vec<PushExpr> = filters.iter().filter_map(|f| classify(f, schema)).collect();
    if nodes.is_empty() {
        return None;
    }
    let json = serde_json::to_string(&nodes).expect("PushExpr serialization cannot fail");
    // A JSON string never contains NUL bytes, so this cannot fail.
    Some(CString::new(json).expect("JSON contains no NUL bytes"))
}

fn compare_op(op: Operator) -> Option<CompareOp> {
    match op {
        Operator::Eq => Some(CompareOp::Eq),
        Operator::NotEq => Some(CompareOp::Neq),
        Operator::Lt => Some(CompareOp::Lt),
        Operator::LtEq => Some(CompareOp::Lteq),
        Operator::Gt => Some(CompareOp::Gt),
        Operator::GtEq => Some(CompareOp::Gteq),
        _ => None,
    }
}

fn column_ref(expr: &Expr, schema: &Schema) -> Option<ColumnRef> {
    match expr {
        Expr::Column(column) => {
            let index = schema.index_of(&column.name).ok()?;
            Some(ColumnRef {
                name: column.name.clone(),
                index,
            })
        }
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<Literal> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_literal(scalar),
        _ => None,
    }
}

fn scalar_to_literal(scalar: &ScalarValue) -> Option<Literal> {
    let timestamp = |value: &Option<i64>, unit: &'static str, tz: &Option<std::sync::Arc<str>>| {
        value.map(|value| Literal::Timestamp {
            value,
            unit,
            tz: tz.as_ref().map(|tz| tz.to_string()),
        })
    };
    match scalar {
        ScalarValue::Boolean(Some(value)) => Some(Literal::Bool { value: *value }),
        ScalarValue::Int8(Some(value)) => Some(Literal::Int8 { value: *value }),
        ScalarValue::Int16(Some(value)) => Some(Literal::Int16 { value: *value }),
        ScalarValue::Int32(Some(value)) => Some(Literal::Int32 { value: *value }),
        ScalarValue::Int64(Some(value)) => Some(Literal::Int64 { value: *value }),
        ScalarValue::UInt8(Some(value)) => Some(Literal::Uint8 { value: *value }),
        ScalarValue::UInt16(Some(value)) => Some(Literal::Uint16 { value: *value }),
        ScalarValue::UInt32(Some(value)) => Some(Literal::Uint32 { value: *value }),
        ScalarValue::UInt64(Some(value)) => Some(Literal::Uint64 { value: *value }),
        ScalarValue::Float32(Some(value)) => Some(Literal::Float32 { value: *value }),
        ScalarValue::Float64(Some(value)) => Some(Literal::Float64 { value: *value }),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(Literal::Utf8 {
            value: value.clone(),
        }),
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::BinaryView(Some(value)) => Some(Literal::Binary {
            value: base64::engine::general_purpose::STANDARD.encode(value),
        }),
        ScalarValue::Date32(Some(value)) => Some(Literal::Date32 { value: *value }),
        ScalarValue::Date64(Some(value)) => Some(Literal::Date64 { value: *value }),
        ScalarValue::TimestampSecond(value, tz) => timestamp(value, "s", tz),
        ScalarValue::TimestampMillisecond(value, tz) => timestamp(value, "ms", tz),
        ScalarValue::TimestampMicrosecond(value, tz) => timestamp(value, "us", tz),
        ScalarValue::TimestampNanosecond(value, tz) => timestamp(value, "ns", tz),
        // Null literals, decimals, and nested types are deliberately not
        // admitted in this change (design D2).
        _ => None,
    }
}
