//go:build cgo

package native

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestCloseInterruptsNativeRead(t *testing.T) {
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
	stmt, err := conn.Prepare("select sum(value % 7) from range(1000000000)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := stmt.ExecuteArrow(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := reader.(*resultReader)
	defer func() { _ = r.Close() }()
	readDone := make(chan error, 1)
	go func() {
		batch, err := r.Read()
		if batch != nil {
			batch.Release()
		}
		readDone <- err
	}()
	// Wait until Read is inside the native call. A separate context cancellation
	// is only a watchdog; Close itself must interrupt the computation.
	deadline := time.Now().Add(2 * time.Second)
	for r.mu.TryLock() {
		r.mu.Unlock()
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("Read never entered its native call")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		<-closed
		t.Fatal("Close waited for Read instead of canceling it")
	}
	if err := <-readDone; err == nil {
		t.Fatal("query completed instead of being interrupted")
	}
	if ctx.Err() != nil {
		t.Fatal("Close needed external context cancellation")
	}
	if r.stream != nil || r.array != nil || r.result != nil || r.token.ptr != nil {
		t.Fatal("Close retained native resources")
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
