// Package pipe is an experimental, interface-driven take on flow: you implement
// the ends (Producer, Processor, Consumer) and pipe owns the concurrency,
// ordering, back-pressure and cancellation. Inspired by Elixir's GenStage, but
// with push+blocking back-pressure and global order reconstruction.
//
// This is a prototype exploring ergonomics; it is not the stable flow API.
package pipe

import "context"

// Producer emits values into the pipeline via emit until it is done or errors.
type Producer[T any] interface {
	Produce(ctx context.Context, emit func(T) error) error
}

// Processor turns one input value into zero or more outputs via emit. Emitting
// once is Map, emitting conditionally is Filter, emitting many is FlatMap.
type Processor[I, O any] interface {
	Process(ctx context.Context, in I, emit func(O) error) error
}

// Consumer receives each output value. It is never called concurrently, so
// implementations need no synchronization.
type Consumer[T any] interface {
	Consume(ctx context.Context, v T) error
}

// Function adapters (the http.HandlerFunc trick): pass a closure where an
// interface is expected.

type ProducerFunc[T any] func(ctx context.Context, emit func(T) error) error

func (f ProducerFunc[T]) Produce(ctx context.Context, emit func(T) error) error {
	return f(ctx, emit)
}

type ProcessorFunc[I, O any] func(ctx context.Context, in I, emit func(O) error) error

func (f ProcessorFunc[I, O]) Process(ctx context.Context, in I, emit func(O) error) error {
	return f(ctx, in, emit)
}

type ConsumerFunc[T any] func(ctx context.Context, v T) error

func (f ConsumerFunc[T]) Consume(ctx context.Context, v T) error {
	return f(ctx, v)
}

// Then composes two processors with no channel between them: it is plain
// function composition through emit, so multi-stage pipelines run in-process
// inside each worker.
func Then[A, B, C any](p1 Processor[A, B], p2 Processor[B, C]) Processor[A, C] {
	return ProcessorFunc[A, C](func(ctx context.Context, in A, emit func(C) error) error {
		return p1.Process(ctx, in, func(mid B) error {
			return p2.Process(ctx, mid, emit)
		})
	})
}
