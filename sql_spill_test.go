//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestSQLStreamingMemorySpill(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoError(t, db) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)
	for _, query := range []string{
		"SET datafusion.execution.target_partitions = 4",
		"SET datafusion.execution.batch_size = 128",
		"SET datafusion.runtime.memory_limit = '1M'",
	} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	var count, total int64
	err = conn.QueryRowContext(ctx, `SELECT count(*), sum(total) FROM (
		SELECT (v * 7) % 100000 AS k, sum(v) AS total
		FROM generate_series(1, 100000) AS t(v)
		GROUP BY (v * 7) % 100000)`).Scan(&count, &total)
	if err != nil {
		t.Fatal(err)
	}
	if count != 100000 || total != 5000050000 {
		t.Fatalf("spill lost rows: count=%d sum=%d", count, total)
	}
}
