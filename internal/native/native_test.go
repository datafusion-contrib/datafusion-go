//go:build cgo

package native

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestNativeErrorHasKind(t *testing.T) {
	_, err := OpenDatabase(":memory:?datafusion.nope=1")
	if err == nil {
		t.Fatal("expected invalid DataFusion config error")
	}

	var nativeErr *Error
	if !errors.As(err, &nativeErr) {
		t.Fatalf("got %T, want *native.Error", err)
	}
	if nativeErr.Kind == "" {
		t.Fatalf("native error kind is empty for %q", nativeErr.Message)
	}
}

func TestConcurrentStatementExecuteArrowUsesPerCallParameters(t *testing.T) {
	db, err := OpenDatabase("")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Connect(false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select $1 + 1000")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			reader, err := stmt.ExecuteArrow(ctx, []driver.NamedValue{{
				Ordinal: 1,
				Value:   int64(i),
			}})
			if err != nil {
				errs <- err
				return
			}
			defer func() {
				if closer, ok := reader.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}()

			rec, err := reader.Read()
			if err != nil {
				errs <- err
				return
			}
			defer rec.Release()

			got := rec.Column(0).(*array.Int64).Value(0)
			want := int64(i + 1000)
			if got != want {
				errs <- fmt.Errorf("query %d got %d, want %d", i, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}
