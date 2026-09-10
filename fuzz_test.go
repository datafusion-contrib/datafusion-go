//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Values are supplied as parameters, never interpolated into SQL. Each mode
// must return the original values, providing an oracle independent of either
// execution path's implementation.
func FuzzQueryBindings(f *testing.F) {
	for _, seed := range []struct {
		n int64
		s string
	}{
		{0, ""}, {-1, "snow 雪 ? $1"}, {math.MaxInt64, "\x00"}, {math.MinInt64, "'quoted'"},
	} {
		f.Add(seed.n, seed.s)
	}
	db, err := sql.Open("datafusion", "")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = db.Close() })
	const query = "select cast($1 as bigint) as n, cast($2 as varchar) as s"
	stmt, err := db.Prepare(query)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = stmt.Close() })
	f.Fuzz(func(t *testing.T, n int64, text string) {
		if len(text) > 4096 || !utf8.ValidString(text) {
			t.Skip()
		}
		for _, row := range []*sql.Row{db.QueryRow(query, n, text), stmt.QueryRow(n, text)} {
			var gotN int64
			var gotText string
			if err := row.Scan(&gotN, &gotText); err != nil || gotN != n || gotText != text {
				t.Fatalf("SQL round trip: (%d, %q), %v; want (%d, %q)", gotN, gotText, err, n, text)
			}
		}
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer closeNoError(t, conn)
		reader, err := QueryArrowContext(context.Background(), conn, query, n, text)
		if err != nil {
			t.Fatal(err)
		}
		defer closeNoError(t, reader)
		batch, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		defer batch.Release()
		if batch.NumRows() != 1 || batch.Column(0).(*array.Int64).Value(0) != n || batch.Column(1).ValueStr(0) != text {
			t.Fatal("Arrow round trip differs from input parameters")
		}
	})
}
