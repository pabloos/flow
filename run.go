package flow

import (
	"context"
	"sync"
)

type config struct {
	workers int
	ordered bool
}

// Option configures a Run.
type Option func(*config)

// Workers sets the size of the processor pool (parallelism). Default 1.
func Workers(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.workers = n
		}
	}
}

// Ordered reconstructs the original input order at the consumer, even with a
// worker pool. Without it, outputs are consumed in arrival order.
func Ordered() Option {
	return func(c *config) { c.ordered = true }
}

type seqItem[T any] struct {
	seq uint64
	val T
}

// batch holds all outputs produced from a single input, tagged with that
// input's sequence number so the collector can restore order.
type batch[O any] struct {
	seq  uint64
	outs []O
}

// Run wires producer -> pool of processors -> consumer and owns the
// concurrency, ordering, back-pressure and fail-fast cancellation. It returns
// the first error from any end, or nil.
func Run[I, O any](ctx context.Context, p Producer[I], proc Processor[I, O], c Consumer[O], opts ...Option) error {
	cfg := config{workers: 1}
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = err
				cancel()
			})
		}
	}

	in := make(chan seqItem[I])
	done := make(chan batch[O])

	// Producer: assigns a contiguous sequence number to every value. emit
	// blocks (back-pressure) until a worker is ready.
	var prodWG sync.WaitGroup
	prodWG.Add(1)
	go func() {
		defer prodWG.Done()
		defer close(in)
		var seq uint64
		err := p.Produce(ctx, func(v I) error {
			select {
			case in <- seqItem[I]{seq: seq, val: v}:
				seq++
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() == nil {
			fail(err)
		}
	}()

	// Worker pool: each input is processed fully by one worker, whose outputs
	// are gathered into a single batch (contiguous, so order can be restored).
	var workWG sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		workWG.Add(1)
		go func() {
			defer workWG.Done()
			for it := range in {
				var outs []O
				err := proc.Process(ctx, it.val, func(o O) error {
					outs = append(outs, o)
					return nil
				})
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				select {
				case done <- batch[O]{seq: it.seq, outs: outs}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		workWG.Wait()
		close(done)
	}()

	// Collector: the single goroutine that calls the consumer. Delivers in
	// arrival order, or reorders by sequence when Ordered is set.
	stopped := false
	deliver := func(o O) {
		if stopped {
			return
		}
		if err := c.Consume(ctx, o); err != nil {
			fail(err)
			stopped = true
		}
	}

	if cfg.ordered {
		next := uint64(0)
		pending := make(map[uint64][]O)
		for b := range done {
			pending[b.seq] = b.outs
			for {
				outs, ok := pending[next]
				if !ok {
					break
				}
				for _, o := range outs {
					deliver(o)
				}
				delete(pending, next)
				next++
			}
		}
	} else {
		for b := range done {
			for _, o := range b.outs {
				deliver(o)
			}
		}
	}

	prodWG.Wait()
	return firstErr
}
