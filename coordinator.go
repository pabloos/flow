package flow

import (
	"context"
	"sync"
)

type coordKey struct{}

type coordinator struct {
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

func (c *coordinator) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
		c.cancel()
	}
}

func (c *coordinator) firstErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// New derives a flow-aware context that every Source and Stage in the same
// pipeline must share. It is what makes fail-fast work: the first stage that
// returns an error cancels this context, unwinding the whole pipeline, and the
// error surfaces from the sink.
//
//	ctx, cancel := flow.New(context.Background())
//	defer cancel()
func New(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	c := &coordinator{cancel: cancel}
	return context.WithValue(ctx, coordKey{}, c), cancel
}

func coordOf(ctx context.Context) *coordinator {
	c, _ := ctx.Value(coordKey{}).(*coordinator)
	return c
}

// fail records the first error and cancels the pipeline. No-op if ctx was not
// created with New.
func fail(ctx context.Context, err error) {
	if c := coordOf(ctx); c != nil {
		c.fail(err)
	}
}

// firstError returns the error that aborted the pipeline, if any.
func firstError(ctx context.Context) error {
	if c := coordOf(ctx); c != nil {
		return c.firstErr()
	}
	return nil
}
