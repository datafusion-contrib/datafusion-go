//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
)

func benchmarkDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("datafusion", "")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { closeNoError(b, db) })
	return db
}

func BenchmarkQueryRowScalar(b *testing.B) {
	db := benchmarkDB(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var got int64
		if err := db.QueryRowContext(ctx, "select 1").Scan(&got); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPreparedParameters(b *testing.B) {
	db := benchmarkDB(b)
	ctx := context.Background()
	stmt, err := db.PrepareContext(ctx, "select $1 + $2")
	if err != nil {
		b.Fatal(err)
	}
	defer closeNoError(b, stmt)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var got int64
		if err := stmt.QueryRowContext(ctx, int64(i), int64(1)).Scan(&got); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkArrowReaderRange(b *testing.B) {
	db := benchmarkDB(b)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer closeNoError(b, conn)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := QueryArrowContext(ctx, conn, "select * from range(1024)")
		if err != nil {
			b.Fatal(err)
		}
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				b.Fatal(err)
			}
			record.Release()
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// Keep normalization cost visible as placeholder counts grow.
func BenchmarkPreparePlaceholders(b *testing.B) {
	for _, style := range []string{"dollar", "question"} {
		for _, n := range []int{128, 2048, 8192} {
			b.Run(fmt.Sprintf("%s/%d", style, n), func(b *testing.B) {
				db := benchmarkDB(b)
				ctx := context.Background()
				conn, err := db.Conn(ctx)
				if err != nil {
					b.Fatal(err)
				}
				defer closeNoError(b, conn)
				columns := make([]string, n)
				for i := range columns {
					columns[i] = "?"
					if style == "dollar" {
						columns[i] = fmt.Sprintf("$%d", i+1)
					}
				}
				query := "SELECT " + strings.Join(columns, ",")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					stmt, err := conn.PrepareContext(ctx, query)
					if err != nil {
						b.Fatal(err)
					}
					closeNoError(b, stmt)
				}
			})
		}
	}
}

func BenchmarkPreparedSyntaxCache(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
			connector, err := NewConnectorWithInitContext("", nil, WithPreparedStatementCache(enabled))
			if err != nil {
				b.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer closeNoError(b, db)
			ctx := context.Background()
			stmt, err := db.PrepareContext(ctx, "select $1 + $2")
			if err != nil {
				b.Fatal(err)
			}
			defer closeNoError(b, stmt)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var got int64
				if err := stmt.QueryRowContext(ctx, int64(i), int64(1)).Scan(&got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
