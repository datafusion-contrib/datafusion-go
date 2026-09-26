package native

import "errors"

// Shared sentinels let Arrow-native errors and the database/sql adapter expose
// the same classifications without an import cycle or matching error text.
var (
	ErrCancelled       = errors.New("datafusion native query canceled")
	ErrInvalidArgument = errors.New("datafusion native invalid argument")
	ErrFailure         = errors.New("datafusion native failure")
	ErrPanic           = errors.New("datafusion native panic")
)
