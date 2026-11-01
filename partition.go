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
//
// Strings and the integer types are hashed directly. A uint64 key is taken as
// the hash itself, so returning one is the way to choose the distribution
// yourself:
//
//	flow.RunPartitioned(ctx, prod, proc, cons,
//	    func(e event) uint64 { return myHash(e.Tenant) })
//
// Any other comparable key falls back to hashing its %#v form, which is
// slower; prefer one of the fast paths on a hot pipeline.
func RunPartitioned[I, O any, K comparable](ctx context.Context, p Producer[I], proc Processor[I, O], c Consumer[O], key func(I) K, opts ...Option) error {
	return run(ctx, p, proc, c, func(v I) uint64 { return hashKey(key(v)) }, opts...)
}

// hashKey maps a key onto the worker-selection space. Collisions are safe —
// they only put two keys on one worker, never split one key across workers —
// but they skew the load, so the fast paths hash the whole value.
func hashKey[K comparable](k K) uint64 {
	switch v := any(k).(type) {
	case uint64:
		return v // caller-supplied hash: used as-is
	case string:
		return fnvString(v)
	case int:
		return uint64(v)
	case int64:
		return uint64(v)
	case int32:
		return uint64(v)
	case int16:
		return uint64(v)
	case int8:
		return uint64(v)
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	default:
		// %#v rather than %v: %v flattens a struct's fields into one
		// space-separated line, so {"x y", "z"} and {"x", "y z"} collide.
		h := fnv.New64a()
		fmt.Fprintf(h, "%#v", k)
		return h.Sum64()
	}
}

// fnvString is FNV-1a over a string's bytes, without the allocation that
// hash.Hash plus fmt would cost.
func fnvString(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
