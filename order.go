package flow

// Order controls how CollectOrdered arranges results relative to the input.
type Order int

const (
	// NoOrder returns results in arrival order: fastest, but nondeterministic
	// after a fan-out.
	NoOrder Order = iota
	// InOrder restores the original input order (deterministic).
	InOrder
	// Reverse returns the input order reversed (deterministic).
	Reverse
)
