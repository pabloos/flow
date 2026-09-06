package flow

// Stage transforms one Stream into another, possibly changing the element type.
// The context is captured at construction time.
type Stage[I, O any] func(Stream[I]) Stream[O]
