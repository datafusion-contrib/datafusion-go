package datafusion

import (
	"errors"
	"fmt"
)

// ErrorType identifies the operation that failed.
type ErrorType string

const (
	// ErrorConnect marks failures while opening a database or connection.
	ErrorConnect ErrorType = "connect"
	// ErrorPrepare marks failures while parsing or preparing SQL.
	ErrorPrepare ErrorType = "prepare"
	// ErrorBind marks failures while binding SQL parameters.
	ErrorBind ErrorType = "bind"
	// ErrorExecute marks failures while executing SQL or reading result batches.
	ErrorExecute ErrorType = "execute"
	// ErrorScan marks failures while adapting Arrow values to database/sql rows.
	ErrorScan ErrorType = "scan"
	// ErrorClosed marks use of a closed connector, connection, statement, or reader.
	ErrorClosed ErrorType = "closed"
	// ErrorUnsupported marks operations DataFusion does not support through this driver.
	ErrorUnsupported ErrorType = "unsupported"
	// ErrorNative marks uncategorized native FFI failures.
	ErrorNative ErrorType = "native"
)

// NativeErrorKind is the stable public classification for native DataFusion errors.
type NativeErrorKind string

const (
	// NativeErrorKindCancelled indicates a query canceled through context cancellation.
	NativeErrorKindCancelled NativeErrorKind = "cancelled"
	// NativeErrorKindInvalidArgument indicates invalid SQL, parameters, or API input.
	NativeErrorKindInvalidArgument NativeErrorKind = "invalid_argument"
	// NativeErrorKindNative indicates an uncategorized native DataFusion failure.
	NativeErrorKindNative NativeErrorKind = "native"
	// NativeErrorKindPanic indicates a panic caught on the Rust side of the FFI boundary.
	NativeErrorKindPanic NativeErrorKind = "panic"
)

var (
	// ErrNativeCancelled matches errors caused by native query cancellation.
	ErrNativeCancelled = errors.New("datafusion native query canceled")
	// ErrNativeInvalidArgument matches native invalid-argument errors.
	ErrNativeInvalidArgument = errors.New("datafusion native invalid argument")
	// ErrNativeFailure matches uncategorized native DataFusion failures.
	ErrNativeFailure = errors.New("datafusion native failure")
	// ErrNativePanic matches panics caught on the Rust side of the FFI boundary.
	ErrNativePanic = errors.New("datafusion native panic")
)

// Error is the structured error type returned by this driver.
type Error struct {
	// Type identifies the driver operation that failed.
	Type ErrorType
	// NativeKind identifies native DataFusion failures when available.
	NativeKind NativeErrorKind
	// Message is the driver-level error message.
	Message string
	// Cause is the wrapped native or lower-level error.
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause == nil {
		return e.Message
	}
	if e.Message == "" {
		return e.Cause.Error()
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Cause)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrNativeCancelled:
		return e.NativeKind == NativeErrorKindCancelled
	case ErrNativeInvalidArgument:
		return e.NativeKind == NativeErrorKindInvalidArgument
	case ErrNativeFailure:
		return e.NativeKind == NativeErrorKindNative
	case ErrNativePanic:
		return e.NativeKind == NativeErrorKindPanic
	default:
		return false
	}
}

func driverError(t ErrorType, message string, cause error) error {
	return &Error{
		Type:       t,
		NativeKind: nativeKind(cause),
		Message:    message,
		Cause:      cause,
	}
}

type nativeKindError interface {
	NativeErrorKind() string
}

func nativeKind(err error) NativeErrorKind {
	var nativeErr nativeKindError
	if errors.As(err, &nativeErr) {
		return NativeErrorKind(nativeErr.NativeErrorKind())
	}
	return ""
}
