package flow

// Then composes two stages, respecting the intermediate type B. This is the
// honest way to build a type-changing pipeline (A -> B -> C) in Go, where a
// variadic of type-changing stages is not expressible.
func Then[A, B, C any](first Stage[A, B], second Stage[B, C]) Stage[A, C] {
	return func(in Stream[A]) Stream[C] {
		return second(first(in))
	}
}

// Chain composes same-typed stages left to right. Convenience for the common
// T -> T -> T case, where the variadic does work.
func Chain[T any](stages ...Stage[T, T]) Stage[T, T] {
	return func(in Stream[T]) Stream[T] {
		out := in
		for _, s := range stages {
			out = s(out)
		}
		return out
	}
}
