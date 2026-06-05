package datafusion

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SQLExecerContext is implemented by *sql.DB, *sql.Conn, and *sql.Tx.
type SQLExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ExecStatements executes already-split SQL statements in order.
//
// DataFusion prepares exactly one SQL statement at a time, so callers handling
// migration files should split the script before calling this helper.
func ExecStatements(ctx context.Context, execer SQLExecerContext, statements []string) error {
	for i, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := execer.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("datafusion statement %d: %w", i+1, err)
		}
	}
	return nil
}
