use datafusion_sql::sqlparser::tokenizer::Location;

use super::*;

#[test]
fn rewrites_question_marks_with_unicode_comments_and_multiline_sql() {
    let query = "select '雪?' as literal, ? as first, -- ? comment\n  'Ω' || ? as second, /* ? block */ ? as third";

    let prepared = prepare_query(query.to_owned()).expect("prepare query");

    assert_eq!(prepared.params.count(), 3);
    assert_eq!(
        prepared.query,
        "select '雪?' as literal, $1 as first, -- ? comment\n  'Ω' || $2 as second, /* ? block */ $3 as third"
    );
}

#[test]
fn rejects_malformed_or_mixed_placeholder_variants() {
    for query in [
        "select ?1",
        "select ?, $1",
        "select ?, $value",
        "select $1, $value",
        "select $0",
        "select $",
    ] {
        assert!(
            prepare_query(query.to_owned()).is_err(),
            "expected {query:?} to fail"
        );
    }
}

#[test]
fn location_offset_handles_multibyte_characters_and_line_endings() {
    let query = "αβ\n雪 ?\nline";

    assert_eq!(
        location_offset(query, Location { line: 2, column: 3 }).expect("offset"),
        "αβ\n雪 ".len()
    );
    assert_eq!(
        location_offset(query, Location { line: 3, column: 5 }).expect("offset"),
        "αβ\n雪 ?\nline".len()
    );
    assert!(
        location_offset(
            query,
            Location {
                line: 99,
                column: 1
            }
        )
        .is_err()
    );
}
