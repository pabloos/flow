package flow

import "context"

// FanOut distributes the input across the given worker stages using sched, and
// returns one output Stream per worker. Every element keeps its origin order,
// so a downstream FanIn + CollectOrdered can restore the input sequence despite
// the concurrency. WithBuffer sizes each worker's input channel, which is what
// lets LeastBusy route around a slow worker.
func FanOut[I, O any](ctx context.Context, in Stream[I], sched Scheduler, workers []Stage[I, O], opts ...Option) []Stream[O] {
	cfg := newConfig(opts)
	ins := make([]Stream[I], len(workers))
	outs := make([]Stream[O], len(workers))
	for i, w := range workers {
		ins[i] = make(Stream[I], cfg.buffer)
		outs[i] = w(ins[i])
	}
	go distribute(ctx, in, ins, sched)
	return outs
}

// FanOutN replicates a single worker stage n times (data parallelism). Applying
// the same Stage value to distinct channels spawns independent goroutines, so
// one worker definition yields n concurrent workers.
func FanOutN[I, O any](ctx context.Context, in Stream[I], n int, sched Scheduler, worker Stage[I, O], opts ...Option) []Stream[O] {
	workers := make([]Stage[I, O], n)
	for i := range workers {
		workers[i] = worker
	}
	return FanOut(ctx, in, sched, workers, opts...)
}

func distribute[I any](ctx context.Context, in Stream[I], outs []Stream[I], sched Scheduler) {
	defer func() {
		for _, c := range outs {
			close(c)
		}
	}()
	loads := make([]int, len(outs))
	var pos uint64
	for el := range in {
		for i, c := range outs {
			loads[i] = len(c)
		}
		target := sched.Route(pos, loads)
		select {
		case outs[target] <- el:
		case <-ctx.Done():
			return
		}
		pos++
	}
}
