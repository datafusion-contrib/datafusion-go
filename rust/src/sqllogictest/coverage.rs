//! Conservative syntax evidence, separate from the upstream result oracle.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion_sql::sqlparser::ast::{
    Expr, GroupByExpr, JoinConstraint, JoinOperator, PipeOperator, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, Visit, Visitor,
};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

#[derive(Default)]
pub(super) struct Syntax {
    pub(super) functions: BTreeSet<String>,
    pub(super) operators: BTreeSet<String>,
    pub(super) clauses: BTreeSet<String>,
    pub(super) query_statement: bool,
}

impl Syntax {
    fn select(&mut self, clause: &str) {
        self.clauses.insert(format!("select.md#{clause}"));
    }
}

impl Visitor for Syntax {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if query.with.is_some() {
            self.select("with-clause");
        }
        if query.order_by.is_some() {
            self.select("order-by-clause");
        }
        if query.limit_clause.is_some() {
            self.select("limit-and-offset-clauses");
        }
        if matches!(&*query.body, SetExpr::SetOperation { .. }) {
            self.select("set-operations");
        }
        for pipe in &query.pipe_operators {
            self.select("pipe-operators");
            let name = match pipe {
                PipeOperator::Where { .. } => "where",
                PipeOperator::OrderBy { .. } => "order-by",
                PipeOperator::Limit { .. } => "limit",
                PipeOperator::Select { .. } => "select",
                PipeOperator::Extend { .. } => "extend",
                PipeOperator::As { .. } => "as",
                PipeOperator::Union { .. } => "union",
                PipeOperator::Intersect { .. } => "intersect",
                PipeOperator::Except { .. } => "except",
                PipeOperator::Aggregate { .. } => "aggregate",
                PipeOperator::Join { .. } => "join",
                _ => continue,
            };
            self.select(name);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        self.select("select-syntax");
        self.select("select-clause");
        if select.distinct.is_some() {
            self.select("distinct");
        }
        if select.into.is_some() {
            self.select("select-into");
        }
        if !select.from.is_empty() {
            self.select("from-clause");
        }
        if select.selection.is_some() {
            self.select("where-clause");
        }
        if select.having.is_some() {
            self.select("having-clause");
        }
        if select.qualify.is_some() {
            self.select("qualify-clause");
        }
        if !select.named_window.is_empty() {
            self.select("window-clause");
        }
        if !matches!(&select.group_by, GroupByExpr::Expressions(expressions, _) if expressions.is_empty())
        {
            self.select("group-by-clause");
        }
        if select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            )
        }) {
            self.select("wildcards");
        }
        for table in &select.from {
            for join in &table.joins {
                self.select("join-clause");
                let (name, constraint) = match &join.join_operator {
                    JoinOperator::Join(c) | JoinOperator::Inner(c) => ("inner-join", c),
                    JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => ("left-outer-join", c),
                    JoinOperator::Right(c) | JoinOperator::RightOuter(c) => ("right-outer-join", c),
                    JoinOperator::FullOuter(c) => ("full-outer-join", c),
                    JoinOperator::CrossJoin(c) => ("cross-join", c),
                    JoinOperator::Semi(c) | JoinOperator::LeftSemi(c) => ("left-semi-join", c),
                    JoinOperator::RightSemi(c) => ("right-semi-join", c),
                    JoinOperator::Anti(c) | JoinOperator::LeftAnti(c) => ("left-anti-join", c),
                    JoinOperator::RightAnti(c) => ("right-anti-join", c),
                    _ => continue,
                };
                self.select(name);
                if matches!(constraint, JoinConstraint::Natural) {
                    self.select("natural-join");
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        if matches!(
            table,
            TableFactor::Derived { lateral: true, .. }
                | TableFactor::Function { lateral: true, .. }
        ) {
            self.select("lateral-join");
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let name = match expr {
            Expr::Function(function) => {
                let name = function.name.to_string().to_lowercase();
                if function.over.is_some() {
                    self.functions.insert(format!("window:{name}"));
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
            self.functions.insert(name);
        }
        match expr {
            Expr::BinaryOp { op, .. } => {
                self.operators.insert(op.to_string());
            }
            Expr::IsDistinctFrom(_, _) => {
                self.operators.insert("IS DISTINCT FROM".into());
            }
            Expr::IsNotDistinctFrom(_, _) => {
                self.operators.insert("IS NOT DISTINCT FROM".into());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn syntax(sql: &str) -> Result<Syntax, String> {
    let statements = Parser::parse_sql(&GenericDialect, sql).map_err(|e| e.to_string())?;
    let mut visitor = Syntax {
        query_statement: matches!(statements.as_slice(), [Statement::Query(_)]),
        ..Syntax::default()
    };
    let _ = statements.visit(&mut visitor);
    Ok(visitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_calls_and_special_syntax_without_counting_comments_or_strings() {
        let found = syntax(
            "SELECT abs(-1), substring('sqrt(4)' FROM 2), sum(x) OVER () FROM (VALUES (1)) t(x) -- sin(0)",
        ).unwrap();
        assert!(found.operators.is_empty());
        assert_eq!(
            found.functions,
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
        assert!(syntax("SELECT abs(").is_err());
    }

    #[test]
    fn distinguishes_explain_after_comments_from_value_queries() {
        assert!(
            !syntax("/* plan only */ EXPLAIN SELECT abs(-1)")
                .unwrap()
                .query_statement
        );
        let found = syntax("WITH x AS (VALUES (1), (2)) SELECT v, count(*) OVER w FROM x AS t(v) WHERE v > 0 WINDOW w AS () ORDER BY v LIMIT 1").unwrap();
        assert!(found.query_statement);
        for clause in [
            "with-clause",
            "where-clause",
            "window-clause",
            "order-by-clause",
            "limit-and-offset-clauses",
        ] {
            assert!(found.clauses.contains(&format!("select.md#{clause}")));
        }
        let found = syntax("SELECT 'WHERE GROUP BY WINDOW JOIN' AS text").unwrap();
        assert_eq!(
            found.clauses,
            BTreeSet::from([
                "select.md#select-syntax".into(),
                "select.md#select-clause".into()
            ])
        );
    }

    #[test]
    fn finds_operators_without_counting_literals() {
        let found = syntax(
            "SELECT 1 + 2, 1 IS DISTINCT FROM NULL, 2 IS NOT DISTINCT FROM 2, 'a + b != c' -- x - y",
        ).unwrap();
        assert_eq!(
            found.operators,
            BTreeSet::from([
                "+".into(),
                "IS DISTINCT FROM".into(),
                "IS NOT DISTINCT FROM".into()
            ])
        );
    }
}
