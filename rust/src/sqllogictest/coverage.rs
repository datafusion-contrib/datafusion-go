//! Conservative syntax evidence, separate from the upstream result oracle.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion_sql::sqlparser::ast::{Expr, Visit, Visitor};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

#[derive(Default)]
struct Functions(BTreeSet<String>);

impl Visitor for Functions {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let name = match expr {
            Expr::Function(function) => {
                let name = function.name.to_string().to_lowercase();
                if function.over.is_some() {
                    self.0.insert(format!("window:{name}"));
                }
                Some(name)
            }
            Expr::Ceil { .. } => Some("ceil".into()),
            Expr::Floor { .. } => Some("floor".into()),
            Expr::Extract { .. } => Some("extract".into()),
            Expr::Position { .. } => Some("position".into()),
            Expr::Substring { shorthand, .. } => {
                Some(if *shorthand { "substr" } else { "substring" }.into())
            }
            Expr::Trim { .. } => Some("trim".into()),
            Expr::Overlay { .. } => Some("overlay".into()),
            Expr::Struct { .. } => Some("struct".into()),
            _ => None,
        };
        if let Some(name) = name {
            self.0.insert(name);
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn functions(sql: &str) -> (BTreeSet<String>, String) {
    match Parser::parse_sql(&GenericDialect, sql) {
        Ok(statements) => {
            let mut visitor = Functions::default();
            let _ = statements.visit(&mut visitor);
            (visitor.0, String::new())
        }
        Err(error) => (BTreeSet::new(), error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_calls_and_special_syntax_without_counting_comments_or_strings() {
        let (names, error) = functions(
            "SELECT abs(-1), substring('sqrt(4)' FROM 2), sum(x) OVER () FROM (VALUES (1)) t(x) -- sin(0)",
        );
        assert!(error.is_empty());
        assert_eq!(
            names,
            BTreeSet::from([
                "abs".into(),
                "substring".into(),
                "sum".into(),
                "window:sum".into()
            ])
        );
    }

    #[test]
    fn invalid_sql_has_no_function_evidence() {
        let (names, error) = functions("SELECT abs(");
        assert!(names.is_empty());
        assert!(!error.is_empty());
    }
}
