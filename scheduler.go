package flow

import "math/rand/v2"

// Scheduler decides which worker receives each element during a FanOut. It
// operates purely on indices and per-worker load, so it stays independent of
// the element type: the typed channel mechanics live in FanOut. Route is called
// once per element with a monotonically increasing position and the current
// queue length of each worker's input channel (len(loads) == number of
// workers). It must be safe to reuse across pipelines.
type Scheduler interface {
	Route(position uint64, loads []int) int
}

type roundRobin struct{}

// Route cycles through workers in order: 0, 1, ..., n-1, 0, 1, ...
func (roundRobin) Route(position uint64, loads []int) int {
	return int(position % uint64(len(loads)))
}

// RoundRobin distributes elements evenly across workers, one after another.
func RoundRobin() Scheduler { return roundRobin{} }

type random struct{}

// Route picks a worker uniformly at random.
func (random) Route(_ uint64, loads []int) int {
	return rand.IntN(len(loads))
}

// Random spreads elements across workers with no ordering guarantee on which
// worker handles what. Cheap and stateless.
func Random() Scheduler { return random{} }

type leastBusy struct{}

// Route sends to the worker with the shortest input queue, breaking ties toward
// the lowest index.
func (leastBusy) Route(_ uint64, loads []int) int {
	best, min := 0, loads[0]
	for i := 1; i < len(loads); i++ {
		if loads[i] < min {
			best, min = i, loads[i]
		}
	}
	return best
}

// LeastBusy routes each element to the least-loaded worker, so a slow worker
// stops blocking the others (the fix for RoundRobin head-of-line stalling).
// Only meaningful with buffered worker channels (FanOut/FanOutN + WithBuffer);
// with unbuffered channels every load is 0 and it degrades to always picking
// worker 0.
func LeastBusy() Scheduler { return leastBusy{} }
