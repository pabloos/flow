package pipe

import "context"

// Map builds a Processor from a plain transform. Use TryMap if it can fail.
func Map[I, O any](fn func(I) O) Processor[I, O] {
	return ProcessorFunc[I, O](func(ctx context.Context, in I, emit func(O) error) error {
		return emit(fn(in))
	})
}

// TryMap builds a Processor from a fallible transform; a returned error aborts
// the pipeline (fail-fast).
func TryMap[I, O any](fn func(I) (O, error)) Processor[I, O] {
	return ProcessorFunc[I, O](func(ctx context.Context, in I, emit func(O) error) error {
		o, err := fn(in)
		if err != nil {
			return err
		}
		return emit(o)
	})
}

// Filter builds a Processor that keeps values for which pred is true.
func Filter[T any](pred func(T) bool) Processor[T, T] {
	return ProcessorFunc[T, T](func(ctx context.Context, in T, emit func(T) error) error {
		if pred(in) {
			return emit(in)
		}
		return nil
	})
}

// TryFilter is Filter with a fallible predicate.
func TryFilter[T any](pred func(T) (bool, error)) Processor[T, T] {
	return ProcessorFunc[T, T](func(ctx context.Context, in T, emit func(T) error) error {
		ok, err := pred(in)
		if err != nil {
			return err
		}
		if ok {
			return emit(in)
		}
		return nil
	})
}

// FlatMap builds a Processor that expands each value into zero or more outputs.
func FlatMap[I, O any](fn func(I) []O) Processor[I, O] {
	return ProcessorFunc[I, O](func(ctx context.Context, in I, emit func(O) error) error {
		for _, o := range fn(in) {
			if err := emit(o); err != nil {
				return err
			}
		}
		return nil
	})
}

// TryFlatMap is FlatMap with a fallible expansion.
func TryFlatMap[I, O any](fn func(I) ([]O, error)) Processor[I, O] {
	return ProcessorFunc[I, O](func(ctx context.Context, in I, emit func(O) error) error {
		outs, err := fn(in)
		if err != nil {
			return err
		}
		for _, o := range outs {
			if err := emit(o); err != nil {
				return err
			}
		}
		return nil
	})
}
