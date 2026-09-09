package native

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/arrio"
)

// RecordReader owns a native result stream. Its schema is available before the
// first read, including for empty results. Close is idempotent and cancels work.
// Each returned batch has its own reference and must be released by the caller.
type RecordReader interface {
	arrio.Reader
	Schema() *arrow.Schema
	Close() error
}
