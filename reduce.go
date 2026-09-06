package flow

import "context"

// Reduce folds the stream into a single accumulated value.
func Reduce[T, A any](ctx context.Context, in Stream[T], init A, fn func(A, T) A) (A, error) {
	acc := init
	for el := range in {
		acc = fn(acc, el.Value)
	}
	return acc, firstError(ctx)
}
