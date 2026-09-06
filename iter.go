package flow

import (
	"context"
	"iter"
)

// From bridges a Go 1.23 iterator (iter.Seq) into a Stream, consuming it lazily.
func From[T any](ctx context.Context, seq iter.Seq[T], opts ...Option) Stream[T] {
	cfg := newConfig(opts)
	out := make(Stream[T], cfg.buffer)
	go func() {
		defer close(out)
		var i uint64
		for v := range seq {
			select {
			case out <- Element[T]{Value: v, order: i}:
			case <-ctx.Done():
				return
			}
			i++
		}
	}()
	return out
}

// Seq bridges a Stream back into a Go 1.23 iterator. Pair the pipeline with
// `defer cancel()` so that breaking out of the range unwinds upstream stages.
func Seq[T any](in Stream[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for el := range in {
			if !yield(el.Value) {
				return
			}
		}
	}
}
