package flow

// Stream is the typed channel that connects stages.
type Stream[T any] chan Element[T]
