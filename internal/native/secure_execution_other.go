//go:build cgo && !linux && !darwin

package native

func secureExecutionPlatform() bool {
	return false
}
