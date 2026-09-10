//! Independent oracle: execute the shared fixtures directly through DataFusion,
//! without the Go driver, C ABI, placeholder rewriter, or row adapter.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use serde_json::Value;

#[tokio::test]
async fn native_query_corpus() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/conformance");
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "query corpus is empty");
    let mut count = 0;
    for path in paths {
        let cases: Vec<Value> = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        for case in cases {
            count += 1;
            let name = case["name"].as_str().unwrap();
            let ctx = SessionContext::new();
            if let Some(setup) = case["setup"].as_array() {
                for sql in setup {
                    ctx.sql(sql.as_str().unwrap())
                        .await
                        .unwrap()
                        .collect()
                        .await
                        .unwrap();
                }
            }
            let sql = case["native_sql"]
                .as_str()
                .unwrap_or_else(|| case["sql"].as_str().unwrap());
            let result = async {
                let mut plan = ctx.state().create_logical_plan(sql).await?;
                if let Some(parameters) = case["parameters"].as_array() {
                    let values: Vec<_> = parameters.iter().map(parameter).collect();
                    let values = if parameters.iter().any(|p| p["name"].is_string()) {
                        ParamValues::from(
                            parameters
                                .iter()
                                .zip(values)
                                .map(|(p, value)| (p["name"].as_str().unwrap().to_owned(), value))
                                .collect::<HashMap<_, _>>(),
                        )
                    } else {
                        ParamValues::from(values)
                    };
                    plan = plan.with_param_values(values)?;
                }
                let df = ctx.execute_logical_plan(plan).await?;
                let stream = df.execute_stream().await?;
                let schema = stream.schema();
                let batches: Vec<_> = stream.try_collect().await?;
                Ok::<_, datafusion::error::DataFusionError>((schema, batches))
            }
            .await;
            if let Some(expected_error) = case["error"].as_str() {
                let error = result.expect_err(name).to_string();
                assert!(error.contains(expected_error), "{name}: {error}");
                continue;
            }
            let (schema, batches) = result.unwrap_or_else(|e| panic!("{name}: {e}"));
            let columns = case["columns"].as_array().unwrap();
            assert_eq!(schema.fields().len(), columns.len(), "{name}: column count");
            for (field, expected) in schema.fields().iter().zip(columns) {
                assert_eq!(field.name(), expected["name"].as_str().unwrap(), "{name}");
                assert_eq!(
                    type_name(field.data_type()),
                    expected["type"].as_str().unwrap(),
                    "{name}: type"
                );
                assert_eq!(
                    field.is_nullable(),
                    expected["nullable"].as_bool().unwrap(),
                    "{name}: nullable"
                );
            }
            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|arr| {
                                if arr.is_null(row) {
                                    None
                                } else {
                                    let value = array_value_to_string(arr, row).unwrap();
                                    // Compare decimal values independently of each
                                    // Arrow implementation's trailing-zero display.
                                    Some(
                                        if matches!(arr.data_type(), DataType::Decimal128(_, _))
                                            && value.contains('.')
                                        {
                                            value
                                                .trim_end_matches('0')
                                                .trim_end_matches('.')
                                                .to_owned()
                                        } else {
                                            value
                                        },
                                    )
                                }
                            })
                            .collect(),
                    );
                }
            }
            assert_eq!(serde_json::to_value(rows).unwrap(), case["rows"], "{name}");
        }
    }
    assert!(count > 0, "query corpus contains no cases");
}

fn parameter(value: &Value) -> ScalarValue {
    match value["type"].as_str().unwrap() {
        "int64" => ScalarValue::Int64(Some(value["value"].as_str().unwrap().parse().unwrap())),
        "string" => ScalarValue::Utf8(Some(value["value"].as_str().unwrap().to_owned())),
        "uint64" => ScalarValue::UInt64(Some(value["value"].as_str().unwrap().parse().unwrap())),
        "float64" => ScalarValue::Float64(Some(value["value"].as_str().unwrap().parse().unwrap())),
        "bool" => ScalarValue::Boolean(Some(value["value"].as_str().unwrap().parse().unwrap())),
        "binary" => {
            let text = value["value"].as_str().unwrap();
            let bytes = (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect();
            ScalarValue::Binary(Some(bytes))
        }
        "null_int64" => ScalarValue::Int64(None),
        other => panic!("unsupported corpus parameter type {other}"),
    }
}

fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "bool".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Int8 => "int8".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::Utf8View => "string_view".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Decimal128(precision, scale) => format!("decimal128({precision},{scale})"),
        other => panic!("unsupported corpus data type {other}"),
    }
}
