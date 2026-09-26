//go:build cgo

package native

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// Mallocator.AllocatedBytes reads its counter non-atomically in Arrow Go 18.8.
// Keep a separate atomic counter for releases on native callback threads.
type callbackTrackingAllocator struct {
	memory.Allocator
	bytes atomic.Int64
}

func (a *callbackTrackingAllocator) Allocate(n int) []byte {
	b := a.Allocator.Allocate(n)
	a.bytes.Add(int64(len(b)))
	return b
}
func (a *callbackTrackingAllocator) Reallocate(n int, old []byte) []byte {
	b := a.Allocator.Reallocate(n, old)
	a.bytes.Add(int64(len(b) - len(old)))
	return b
}
func (a *callbackTrackingAllocator) Free(b []byte) {
	a.Allocator.Free(b)
	a.bytes.Add(-int64(len(b)))
}
func trackCallbackAllocations(t *testing.T) *callbackTrackingAllocator {
	t.Helper()
	previous := callbackAllocator
	a := &callbackTrackingAllocator{Allocator: mallocator.NewMallocator()}
	callbackAllocator = a
	t.Cleanup(func() { callbackAllocator = previous })
	return a
}

func TestCallbackNativeBuffersFollowRetainedBatch(t *testing.T) {
	alloc := trackCallbackAllocations(t)
	before := alloc.bytes.Load()
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
	f := ScalarFunction{Name: "go_values", ReturnType: arrow.PrimitiveTypes.Int64,
		Evaluate: func(_ context.Context, _ []arrow.Array, rows int) (arrow.Array, error) {
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			for i := 0; i < rows; i++ {
				b.Append(int64(i))
			}
			return b.NewArray(), nil
		},
	}
	if err := conn.RegisterGo(f.Name, f, 10); err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("SELECT go_values() FROM range(1024)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	reader, err := stmt.ExecuteArrow(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reader.Read()
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		batch.Release()
		t.Fatal(err)
	}
	stmt.Close()
	conn.Close()
	db.Close()
	if alloc.bytes.Load() <= before {
		batch.Release()
		t.Fatal("retained result should still own native callback buffers")
	}
	if batch.Column(0).(*array.Int64).Value(17) != 17 {
		t.Error("retained batch corrupted")
	}
	batch.Release()
	deadline := time.Now().Add(5 * time.Second)
	for alloc.bytes.Load() != before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := alloc.bytes.Load(); got != before {
		t.Fatalf("native callback buffer leak: before %d, after %d", before, got)
	}
}

type observedCallbackArray struct {
	arrow.Array
	refs atomic.Int32
	done chan struct{}
}

func (a *observedCallbackArray) Retain() { a.refs.Add(1); a.Array.Retain() }
func (a *observedCallbackArray) Release() {
	a.Array.Release()
	if a.refs.Add(-1) == 0 {
		close(a.done)
	}
}

func TestCallbackCancellationReleasesLateOutput(t *testing.T) {
	alloc := trackCallbackAllocations(t)
	before := alloc.bytes.Load()
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
	started, finish, exported := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	}()
	f := ScalarFunction{Name: "go_late", Arguments: []arrow.DataType{arrow.PrimitiveTypes.Int64}, ReturnType: arrow.PrimitiveTypes.Int64,
		Evaluate: func(_ context.Context, args []arrow.Array, _ int) (arrow.Array, error) {
			close(started)
			<-finish // deliberately return a valid result after cancellation
			args[0].Retain()
			a := &observedCallbackArray{Array: args[0], done: exported}
			a.refs.Store(1)
			return a, nil
		},
	}
	if err := conn.RegisterGo(f.Name, f, 10); err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("SELECT go_late(cast(value as bigint)) FROM range(1024)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	reader, err := stmt.ExecuteArrow(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	readDone := make(chan error, 1)
	go func() {
		batch, err := reader.Read()
		if batch != nil {
			batch.Release()
		}
		readDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not start")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("cancelled query succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not cancel")
	}
	close(finish)
	select {
	case <-exported:
	case <-time.After(5 * time.Second):
		t.Fatal("late output was not released")
	}
	deadline := time.Now().Add(5 * time.Second)
	for alloc.bytes.Load() != before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := alloc.bytes.Load(); got != before {
		t.Fatalf("abandoned callback leaked %d native bytes", got-before)
	}
}
