package flow

// Element is the unit that travels through a Stream. It carries the value, its
// position of origin (order), and a sub-position within a FlatMap expansion
// (sub, zero otherwise). Together (order, sub) let CollectOrdered restore the
// input sequence after a fan-in, including the sibling values FlatMap produces.
type Element[T any] struct {
	Value T
	order uint64
	sub   uint64
}
