//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"testing"
)

// These regressions also run in every normal OS/link-mode job, without the
// upstream fixture library. Grammar settings must affect the Go prepare path.
func TestSQLGrammarSettingsAndDefinitions(t *testing.T) {
	for _, test := range []struct {
		name  string
		dsn   string
		setup []string
		query string
		want  int64
	}{
		{"configured_postgres", "?datafusion.sql_parser.dialect=postgresql", nil, "SELECT 7 # 3", 4},
		{"changed_postgres", "", []string{"SET datafusion.sql_parser.dialect = 'postgresql'"}, "SELECT 1_234", 1234},
		{"changed_duckdb", "", []string{"SET datafusion.sql_parser.dialect = 'DuckDB'", "CREATE TABLE t (s STRUCT(a BIGINT)) AS VALUES ({a: 7})"}, "SELECT s.a FROM t", 7},
		{"sql_prepare", "", []string{"PREPARE stored(BIGINT) AS SELECT $1 + 1"}, "EXECUTE stored(41)", 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("datafusion", test.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, db)
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, conn)
			for _, sql := range test.setup {
				if _, err := conn.ExecContext(context.Background(), sql); err != nil {
					t.Fatal(err)
				}
			}
			stmt, err := conn.PrepareContext(context.Background(), test.query)
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoError(t, stmt)
			var value int64
			if err := stmt.QueryRowContext(context.Background()).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != test.want {
				t.Fatalf("got %d, want %d", value, test.want)
			}
		})
	}
}

func TestExplainSQLPreparedDefinition(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)
	rows, err := db.QueryContext(context.Background(), "EXPLAIN PREPARE stored(BIGINT) AS SELECT $1 + 1")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)
	count := 0
	for rows.Next() {
		var kind, plan string
		if err := rows.Scan(&kind, &plan); err != nil {
			t.Fatal(err)
		}
		if kind == "" || plan == "" {
			t.Fatal("EXPLAIN returned an empty plan")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("EXPLAIN returned no plan")
	}
}
