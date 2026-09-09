//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"
)

func TestSQLStreamingSinglePartitionSpill(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoError(t, db) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)
	for _, query := range []string{
		// Exercise streaming reads during a spill without repartitioning.
		// The full SQL corpus includes the parallel aggregation stress case.
		"SET datafusion.execution.target_partitions = 1",
		"SET datafusion.execution.batch_size = 128",
		"SET datafusion.runtime.memory_limit = '1M'",
	} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	var count, total int64
	const query = `SELECT count(*), sum(total) FROM (
		SELECT (v * 7) % 100000 AS k, sum(v) AS total
		FROM generate_series(1, 100000) AS t(v)
		GROUP BY (v * 7) % 100000)`
	err = conn.QueryRowContext(ctx, query).Scan(&count, &total)
	if err != nil {
		t.Fatal(err)
	}
	if count != 100000 || total != 5000050000 {
		t.Fatalf("spill lost rows: count=%d sum=%d", count, total)
	}
	rows, err := conn.QueryContext(ctx, "EXPLAIN ANALYZE "+query)
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, rows)
	spilled := false
	spillCount := regexp.MustCompile(`spill_count=[1-9][0-9]*`)
	for rows.Next() {
		var planType, plan string
		if err := rows.Scan(&planType, &plan); err != nil {
			t.Fatal(err)
		}
		spilled = spilled || spillCount.MatchString(plan)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !spilled {
		t.Fatal("query completed without exercising aggregation spilling")
	}
}
