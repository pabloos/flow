package flow

import "context"

// Source emits the given items into a new Stream, one Element per item, tagging
// each with its origin position. Unbuffered by design.
func Source[T any](ctx context.Context, items ...T) Stream[T] {
	out := make(Stream[T])
	go func() {
		defer close(out)
		for i, v := range items {
			select {
			case out <- Element[T]{Value: v, order: uint64(i)}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
