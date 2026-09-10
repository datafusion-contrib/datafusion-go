package datafusion

import (
	"context"
	"database/sql/driver"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// queryOperation keeps the two statement ownership policies private: a direct
// query owns a temporary statement, while a prepared query borrows its cached
// statement under Stmt.mu. Both use the same normalization, serialization, and
// result ownership rules below. Preparing an operation never holds a mutex
// while waiting for the connector's serialization lock.
type queryOperation struct {
	connector  *Connector
	serializes bool
	execute    func(context.Context, []driver.NamedValue) (native.RecordReader, error)
	close      func()
}

func runArrowQuery(ctx context.Context, args []driver.NamedValue, prepare func() (queryOperation, error)) (ArrowReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	named, err := normalizeNamedValueSlice(args)
	if err != nil {
		return nil, err
	}
	op, err := prepare()
	if err != nil {
		return nil, err
	}
	if op.close != nil {
		defer op.close()
	}
	unlock, err := op.connector.lockSerializedStatement(ctx, op.serializes)
	if err != nil {
		return nil, err
	}
	reader, err := op.execute(ctx, named)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, err
	}
	// From this point the result owns the lock until its caller closes it.
	return newSerializedArrowReader(reader, unlock), nil
}

func executeNativeStatement(ctx context.Context, stmt *native.Statement, args []driver.NamedValue) (native.RecordReader, error) {
	reader, err := stmt.ExecuteArrow(ctx, args)
	if err != nil {
		return nil, driverError(ErrorExecute, "could not execute DataFusion statement", err)
	}
	return reader, nil
}

// queryRows transfers reader ownership to the database/sql adapter on success,
// and releases it if the schema cannot be represented by database/sql.
func queryRows(reader ArrowReader) (driver.Rows, error) {
	rows, err := newRows(reader)
	if err != nil {
		closeReader(reader)
		return nil, driverError(ErrorScan, "could not create DataFusion rows", err)
	}
	return rows, nil
}

func positionalNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, value := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return named
}
