//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"io"
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
