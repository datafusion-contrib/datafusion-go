package datafusion

import (
	"context"
	"sync"
)

// contextMutex is a zero-value-ready mutex whose wait honors cancellation.
type contextMutex struct {
	once sync.Once
	held chan struct{}
}

func (m *contextMutex) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.once.Do(func() { m.held = make(chan struct{}, 1) })
	select {
	case m.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *contextMutex) Unlock() { <-m.held }
