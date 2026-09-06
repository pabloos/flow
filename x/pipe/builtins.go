package pipe

import (
	"context"
	"iter"
)

// Slice produces the given values in order.
func Slice[T any](xs ...T) Producer[T] {
	return ProducerFunc[T](func(ctx context.Context, emit func(T) error) error {
		for _, x := range xs {
			if err := emit(x); err != nil {
				return err
			}
		}
		return nil
	})
}

// FromSeq produces the values of a Go 1.23 iterator.
func FromSeq[T any](seq iter.Seq[T]) Producer[T] {
	return ProducerFunc[T](func(ctx context.Context, emit func(T) error) error {
		for x := range seq {
			if err := emit(x); err != nil {
				return err
			}
		}
		return nil
	})
}

// Into collects outputs into the given slice. Safe without locks: the consumer
// is called from a single goroutine.
func Into[T any](dst *[]T) Consumer[T] {
	return ConsumerFunc[T](func(ctx context.Context, v T) error {
		*dst = append(*dst, v)
		return nil
	})
}

// Each runs fn for every output.
func Each[T any](fn func(T) error) Consumer[T] {
	return ConsumerFunc[T](func(ctx context.Context, v T) error {
		return fn(v)
	})
}
