package flow

import "context"

// ForEach consumes the stream, applying fn to each value. If fn returns an
// error it aborts the pipeline.
func ForEach[T any](ctx context.Context, in Stream[T], fn func(T) error) error {
	for el := range in {
		if err := fn(el.Value); err != nil {
			fail(ctx, err)
			for range in { //nolint:revive // drain so upstream can unwind
			}
			return err
		}
	}
	return firstError(ctx)
}
