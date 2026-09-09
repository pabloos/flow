package flow

import (
	"context"
	"fmt"
	"hash/fnv"
)

// RunPartitioned is like Run but routes every input to a worker by key: all
// values with the same key go to the same worker and are processed in order,
// so a given key is never processed concurrently. Use it for per-key ordering
// or for processors that keep per-key state.
//
// Combine with Ordered to also restore the global input order at the consumer;
// on its own, RunPartitioned only guarantees order within each key.
func RunPartitioned[I, O any, K comparable](ctx context.Context, p Producer[I], proc Processor[I, O], c Consumer[O], key func(I) K, opts ...Option) error {
	return run(ctx, p, proc, c, func(v I) uint64 { return hashKey(key(v)) }, opts...)
}

func hashKey[K comparable](k K) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%v", k)
	return h.Sum64()
}
