package flow

import "context"

// MapFunc transforms a value, or fails the whole pipeline (fail-fast).
type MapFunc[I, O any] func(I) (O, error)

// Map applies fn to every element. On the first error it aborts the pipeline.
func Map[I, O any](ctx context.Context, fn MapFunc[I, O], opts ...Option) Stage[I, O] {
	cfg := newConfig(opts)
	return func(in Stream[I]) Stream[O] {
		out := make(Stream[O], cfg.buffer)
		go func() {
			defer close(out)
			for el := range in {
				v, err := fn(el.Value)
				if err != nil {
					fail(ctx, err)
					return
				}
				select {
				case out <- Element[O]{Value: v, order: el.order, sub: el.sub}:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out
	}
}
