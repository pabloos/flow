package flow

import "context"

// FlatMapFunc expands a value into zero or more values, or fails the whole
// pipeline (fail-fast).
type FlatMapFunc[I, O any] func(I) ([]O, error)

// FlatMap applies fn to every element and emits each produced value as its own
// element. Emitted siblings keep their parent's order plus an incrementing
// sub-position, so CollectOrdered restores both the input order and the sibling
// order. Returning an empty slice drops the input (like Filter dropping it).
//
// Note: ordering across nested FlatMaps (a FlatMap feeding another) collapses to
// the inner expansion, since Element carries a single sub-position.
func FlatMap[I, O any](ctx context.Context, fn FlatMapFunc[I, O], opts ...Option) Stage[I, O] {
	cfg := newConfig(opts)
	return func(in Stream[I]) Stream[O] {
		out := make(Stream[O], cfg.buffer)
		go func() {
			defer close(out)
			for el := range in {
				vs, err := fn(el.Value)
				if err != nil {
					fail(ctx, err)
					return
				}
				for j, v := range vs {
					select {
					case out <- Element[O]{Value: v, order: el.order, sub: uint64(j)}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out
	}
}
