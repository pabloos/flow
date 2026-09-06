package flow

import "context"

// FilterFunc reports whether a value should pass through.
type FilterFunc[T any] func(T) (bool, error)

// Filter drops elements for which pred returns false.
func Filter[T any](ctx context.Context, pred FilterFunc[T], opts ...Option) Stage[T, T] {
	cfg := newConfig(opts)
	return func(in Stream[T]) Stream[T] {
		out := make(Stream[T], cfg.buffer)
		go func() {
			defer close(out)
			for el := range in {
				ok, err := pred(el.Value)
				if err != nil {
					fail(ctx, err)
					return
				}
				if !ok {
					continue
				}
				select {
				case out <- el:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out
	}
}
