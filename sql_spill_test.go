//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSQLStreamingSinglePartitionSpill(t *testing.T) {
	testSQLStreamingSpill(t, 1)
}

func testSQLStreamingSpill(t *testing.T, partitions int) {
	t.Helper()
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
		// Four partitions are exercised by the separate spill diagnostic.
		fmt.Sprintf("SET datafusion.execution.target_partitions = %d", partitions),
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
	mode := "mode=Single"
	if partitions > 1 {
		mode = "mode=FinalPartitioned"
	}
	for rows.Next() {
		var planType, plan string
		if err := rows.Scan(&planType, &plan); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(plan, "\n") {
			spilled = spilled || (strings.Contains(line, mode) && spillCount.MatchString(line))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !spilled {
		t.Fatal("query completed without exercising aggregation spilling")
	}
}
