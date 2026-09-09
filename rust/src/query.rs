//! SQL normalization, parameter shape, and statement classification.

use std::collections::{BTreeSet, HashMap};

use datafusion::common::{ParamValues, ScalarValue};
use datafusion_sql::parser::Statement as DFStatement;
use datafusion_sql::sqlparser::ast::Statement as SQLStatement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::error::FfiError;

#[derive(Clone)]
pub(crate) struct Binding {
    // Name is present only for database/sql named arguments.
    pub(crate) name: Option<String>,
    // None means a binding slot was referenced but no value has been supplied.
    // That lets validation produce a targeted "missing value" error.
    pub(crate) value: Option<ScalarValue>,
}

#[derive(Clone, Debug)]
pub(crate) enum ParameterMetadata {
    // No placeholders were found during prepare.
    None,
    // Positional metadata stores the highest required slot. `$3` therefore
    // requires three arguments even if `$1` or `$2` are absent from the SQL.
    Positional { count: i64 },
    // Named metadata stores distinct names. Repeated `$name` placeholders bind
    // from a single sql.Named argument.
    Named { names: BTreeSet<String> },
}

pub(crate) struct PreparedQuery {
    pub(crate) query: String,
    pub(crate) params: ParameterMetadata,
}

impl ParameterMetadata {
    pub(crate) fn count(&self) -> i64 {
        match self {
            Self::None => 0,
            Self::Positional { count } => *count,
            Self::Named { names } => i64::try_from(names.len()).unwrap_or(i64::MAX),
        }
    }
}

pub(crate) fn param_values(
    metadata: &ParameterMetadata,
    bindings: Vec<Binding>,
) -> Result<Option<ParamValues>, FfiError> {
    // Validate the argument shape before handing values to DataFusion. The goal
    // is to report database/sql-friendly errors for mixed named/positional
    // inputs, duplicates, sparse positional bindings, and missing values.
    match metadata {
        ParameterMetadata::None => {
            if bindings.is_empty() {
                Ok(None)
            } else {
                Err(FfiError::invalid_argument(format!(
                    "SQL statement has no placeholders but got {} argument(s); remove the arguments or add ?, $1, or $name placeholders",
                    bindings.len()
                )))
            }
        }
        ParameterMetadata::Positional { count } => {
            // Positional placeholders are dense by contract: `$3` means callers
            // must pass arguments 1, 2, and 3. This matches database/sql's
            // NumInput behavior and avoids surprising sparse binding semantics.
            let count =
                usize::try_from(*count).map_err(|e| FfiError::invalid_argument(e.to_string()))?;
            if bindings.len() != count {
                return Err(FfiError::invalid_argument(format!(
                    "SQL statement expects {count} positional argument(s), got {}; pass exactly {count} plain argument(s) for the ?, $1, $2, ... placeholders",
                    bindings.len()
                )));
            }

            let mut params = Vec::with_capacity(count);
            for (idx, binding) in bindings.into_iter().enumerate() {
                if let Some(name) = binding.name {
                    return Err(FfiError::invalid_argument(format!(
                        "SQL statement uses positional placeholders but got named argument {name}; pass a plain argument instead of sql.Named"
                    )));
                }
                let value = binding.value.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "SQL argument {} has no value; pass a non-missing value or a typed null such as datafusion.NullOf(...)",
                        idx + 1
                    ))
                })?;
                params.push(value);
            }
            Ok(Some(ParamValues::from(params)))
        }
        ParameterMetadata::Named { names } => {
            // For named placeholders, repeated `$name` occurrences share one
            // supplied value. Requiring exact name coverage catches typos before
            // DataFusion sees the query.
            if bindings.len() != names.len() {
                return Err(FfiError::invalid_argument(format!(
                    "SQL statement expects {} named argument(s) {}, got {}; pass matching sql.Named values",
                    names.len(),
                    expected_parameter_list(names),
                    bindings.len()
                )));
            }

            let mut params = HashMap::new();
            let mut seen = BTreeSet::new();
            for (idx, binding) in bindings.into_iter().enumerate() {
                let name = binding.name.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "SQL statement uses named placeholders {}; argument {} is positional, so pass sql.Named(\"name\", value)",
                        expected_parameter_list(names),
                        idx + 1
                    ))
                })?;
                if !names.contains(&name) {
                    return Err(FfiError::invalid_argument(format!(
                        "unexpected named argument {name}; expected one of {}",
                        expected_parameter_list(names)
                    )));
                }
                if !seen.insert(name.clone()) {
                    return Err(FfiError::invalid_argument(format!(
                        "duplicate named argument {name}; pass each named placeholder once"
                    )));
                }
                let value = binding.value.ok_or_else(|| {
                    FfiError::invalid_argument(format!(
                        "named argument {name} has no value; pass a non-missing value or a typed null such as datafusion.NullOf(...)"
                    ))
                })?;
                params.insert(name, value);
            }

            for name in names {
                if !seen.contains(name) {
                    return Err(FfiError::invalid_argument(format!(
                        "missing named argument {name}; pass sql.Named({name:?}, value)"
                    )));
                }
            }

            Ok(Some(ParamValues::from(params)))
        }
    }
}

pub(crate) fn expected_parameter_list(names: &BTreeSet<String>) -> String {
    names
        .iter()
        .map(|name| format!("${name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn prepare_query(query: String) -> Result<PreparedQuery, FfiError> {
    let dialect = GenericDialect {};
    // Use sqlparser's tokenizer instead of scanning strings manually. That
    // keeps placeholders inside string literals and comments untouched.
    let tokens = Tokenizer::new(&dialect, &query)
        .tokenize_with_location()
        .map_err(|e| FfiError::invalid_argument(e.to_string()))?;

    let mut positional_max = 0_i64;
    let mut named = BTreeSet::new();
    let mut question_count = 0_i64;
    let mut replacements = Vec::new();

    for token in tokens {
        let Token::Placeholder(placeholder) = token.token else {
            continue;
        };

        if placeholder == "?" {
            // DataFusion accepts `$1` style positional placeholders, so rewrite
            // database/sql question marks during prepare. Locations from the
            // tokenizer are line/column based and must be converted to byte
            // offsets before slicing the original UTF-8 SQL string.
            question_count += 1;
            replacements.push((
                location_offset(&query, token.span.start)?,
                location_offset(&query, token.span.end)?,
                format!("${question_count}"),
            ));
            continue;
        }

        // `$1` and `$name` placeholders pass through to DataFusion unchanged.
        // Other placeholder syntaxes are rejected here so callers get a stable
        // invalid_argument error rather than parser-dependent behavior later.
        let Some(id) = placeholder.strip_prefix('$') else {
            return Err(FfiError::invalid_argument(format!(
                "unsupported placeholder syntax {placeholder}; use ?, $1, or $name"
            )));
        };
        if id.is_empty() {
            return Err(FfiError::invalid_argument("placeholder name is empty"));
        }

        if id.chars().all(|c| c.is_ascii_digit()) {
            let index = id.parse::<i64>().map_err(|e| {
                FfiError::invalid_argument(format!("invalid placeholder {placeholder}: {e}"))
            })?;
            if index <= 0 {
                return Err(FfiError::invalid_argument(format!(
                    "invalid placeholder {placeholder}; indexes are 1-based"
                )));
            }
            positional_max = positional_max.max(index);
        } else {
            named.insert(id.to_owned());
        }
    }

    if question_count > 0 && (positional_max > 0 || !named.is_empty()) {
        // Mixing placeholder families makes NumInput and database/sql argument
        // normalization ambiguous, especially after `?` gets rewritten to `$n`.
        return Err(FfiError::invalid_argument(
            "mixed question-mark, named, and dollar-numbered parameters are not supported",
        ));
    }
    if positional_max > 0 && !named.is_empty() {
        return Err(FfiError::invalid_argument(
            "mixed named and positional parameters are not supported",
        ));
    }
    if question_count > 0 {
        return Ok(PreparedQuery {
            query: rewrite_query(&query, replacements),
            params: ParameterMetadata::Positional {
                count: question_count,
            },
        });
    }
    if !named.is_empty() {
        return Ok(PreparedQuery {
            query,
            params: ParameterMetadata::Named { names: named },
        });
    }
    if positional_max > 0 {
        return Ok(PreparedQuery {
            query,
            params: ParameterMetadata::Positional {
                count: positional_max,
            },
        });
    }
    Ok(PreparedQuery {
        query,
        params: ParameterMetadata::None,
    })
}

pub(crate) fn statement_serializes(stmt: &DFStatement) -> bool {
    // database/sql serializes operations that may mutate connection/session
    // state. Pure queries and SHOW-like statements can run concurrently on the
    // same prepared statement.
    match stmt {
        DFStatement::Statement(stmt) => sql_statement_serializes(stmt),
        DFStatement::Explain(stmt) => statement_serializes(&stmt.statement),
        _ => true,
    }
}

pub(crate) fn sql_statement_serializes(stmt: &SQLStatement) -> bool {
    match stmt {
        SQLStatement::Query(_)
        | SQLStatement::ExplainTable { .. }
        | SQLStatement::ShowFunctions { .. }
        | SQLStatement::ShowVariable { .. }
        | SQLStatement::ShowStatus { .. }
        | SQLStatement::ShowVariables { .. }
        | SQLStatement::ShowCreate { .. }
        | SQLStatement::ShowColumns { .. }
        | SQLStatement::ShowDatabases { .. }
        | SQLStatement::ShowSchemas { .. }
        | SQLStatement::ShowCharset(_)
        | SQLStatement::ShowObjects(_)
        | SQLStatement::ShowTables { .. }
        | SQLStatement::ShowViews { .. }
        | SQLStatement::ShowCollation { .. } => false,
        SQLStatement::Explain { statement, .. } => sql_statement_serializes(statement),
        _ => true,
    }
}

pub(crate) fn rewrite_query(query: &str, replacements: Vec<(usize, usize, String)>) -> String {
    // Replacements are produced in tokenizer order. Building a new String avoids
    // in-place byte shifting while preserving every untouched byte exactly.
    let mut rewritten = String::with_capacity(query.len() + replacements.len());
    let mut last = 0;
    for (start, end, replacement) in replacements {
        rewritten.push_str(&query[last..start]);
        rewritten.push_str(&replacement);
        last = end;
    }
    rewritten.push_str(&query[last..]);
    rewritten
}

pub(crate) fn location_offset(query: &str, target: Location) -> Result<usize, FfiError> {
    // sqlparser reports line/column positions, while Rust string slicing needs
    // byte offsets. Count Unicode scalar values to find the exact char boundary
    // rather than assuming byte-oriented columns.
    if target.line == 0 && target.column == 0 {
        return Err(FfiError::invalid_argument(
            "placeholder span has empty source location",
        ));
    }

    let mut line = 1_u64;
    let mut column = 1_u64;
    for (idx, ch) in query.char_indices() {
        if line == target.line && column == target.column {
            return Ok(idx);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    if line == target.line && column == target.column {
        return Ok(query.len());
    }

    Err(FfiError::invalid_argument(format!(
        "placeholder span location {target} is outside query text"
    )))
}

#[cfg(test)]
mod tests;
