package flow

import (
	"context"
	"sort"
)

// Collect drains the stream into a slice. For a linear pipeline the order is
// preserved naturally (one goroutine per stage is FIFO). Returns the first
// pipeline error, if any (fail-fast); results may be partial in that case.
func Collect[T any](ctx context.Context, in Stream[T]) ([]T, error) {
	var out []T
	for el := range in {
		out = append(out, el.Value)
	}
	return out, firstError(ctx)
}

// CollectOrdered drains the stream and arranges the values according to order.
// Use it after a FanOut/FanIn to get deterministic results despite the
// concurrency. On a pipeline error it returns nil and the first error
// (fail-fast), since a partial ordered result would be misleading.
func CollectOrdered[T any](ctx context.Context, in Stream[T], order Order) ([]T, error) {
	type item struct {
		value T
		order uint64
		sub   uint64
	}

	var items []item
	for el := range in {
		items = append(items, item{value: el.Value, order: el.order, sub: el.sub})
	}
	if err := firstError(ctx); err != nil {
		return nil, err
	}

	switch order {
	case InOrder:
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].order != items[j].order {
				return items[i].order < items[j].order
			}
			return items[i].sub < items[j].sub
		})
	case Reverse:
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].order != items[j].order {
				return items[i].order > items[j].order
			}
			return items[i].sub > items[j].sub
		})
	}

	out := make([]T, len(items))
	for i := range items {
		out[i] = items[i].value
	}
	return out, nil
}
