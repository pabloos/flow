package flow

import (
	"context"
	"sync"
	"time"
)

type config struct {
	workers  int
	ordered  bool
	prefetch int
	observe  *Report
	meter    Meter
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

// Prefetch sets how many items may be queued ahead of the workers, letting the
// producer run ahead instead of lock-stepping with the pool. Default 0
// (unbuffered: strict lock-step).
//
// Run queues them in one shared channel, so the in-flight window is n. In
// RunPartitioned each worker owns its queue — that is what keeps one slow key
// from stalling the others — so the window is n per worker, n*Workers in total.
func Prefetch(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.prefetch = n
		}
	}
}

// Observe fills dst with a performance Report after the run: per-stage timings,
// counts and a diagnosed bottleneck. Opt-in — it adds timing to the hot path,
// so leave it off in production.
//
// Experimental: the observability API (Observe and Report) may change in a
// future v0.x release.
func Observe(dst *Report) Option {
	return func(c *config) { c.observe = dst }
}

// StageError wraps the error that stopped a run, naming the end it came from.
// Run returns one whenever a producer, processor or consumer fails, so callers
// can tell whose fault it was without instrumenting their own ends:
//
//	var se *flow.StageError
//	if errors.As(err, &se) && se.Stage == flow.StageConsumer { ... }
//
// The wrapped error stays reachable through errors.Is and errors.Unwrap. A run
// stopped by its caller's context returns that context error unwrapped, since
// no stage failed.
type StageError struct {
	Stage Stage
	Err   error
}

func (e *StageError) Error() string { return e.Stage.String() + ": " + e.Err.Error() }
func (e *StageError) Unwrap() error { return e.Err }

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
	return run(ctx, p, proc, c, nil, opts...)
}

// run is the shared engine. When partition is non-nil, each input is routed to
// a dedicated worker channel by partition(value) % workers; otherwise all
// workers share one channel (work-stealing).
func run[I, O any](ctx context.Context, p Producer[I], proc Processor[I, O], c Consumer[O], partition func(I) uint64, opts ...Option) error {
	cfg := config{workers: 1}
	for _, opt := range opts {
		opt(&cfg)
	}

	// parent is kept so we can tell "the caller cancelled us" apart from
	// "we cancelled ourselves after an error": fail() cancels ctx, never parent.
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		pr    *probe
		start time.Time
	)
	if cfg.observe != nil {
		pr = newProbe(cfg.workers)
		start = time.Now()
	}

	var mi *meterInstruments
	if cfg.meter != nil {
		mi = newMeterInstruments(cfg.meter)
	}
	observing := cfg.observe != nil || cfg.meter != nil

	var (
		errOnce  sync.Once
		firstErr error
	)
	fail := func(stage Stage, err error) {
		if err != nil {
			errOnce.Do(func() {
				firstErr = &StageError{Stage: stage, Err: err}
				cancel()
			})
		}
	}

	// Intake. In the partitioned path every worker already owns a Prefetch-sized
	// queue, so buffering the intake as well would apply Prefetch twice and hold
	// Prefetch*(Workers+1) items; keep it unbuffered there and let the per-worker
	// queues be the whole window.
	intake := cfg.prefetch
	if partition != nil {
		intake = 0
	}
	in := make(chan seqItem[I], intake)
	done := make(chan batch[O])

	// Producer: assigns a contiguous sequence number to every value. emit
	// blocks (back-pressure) until there is room; that block is measured.
	var prodWG sync.WaitGroup
	prodWG.Add(1)
	go func() {
		defer prodWG.Done()
		defer close(in)
		var seq uint64
		err := p.Produce(ctx, func(v I) error {
			// A context that is already dead must consume nothing: without this,
			// the select below can still pick the send when a worker is parked,
			// leaking one value past cancellation.
			if err := ctx.Err(); err != nil {
				return err
			}
			t := pr.now()
			select {
			case in <- seqItem[I]{seq: seq, val: v}:
				pr.prodBlockedSince(t)
				pr.incProduced()
				mi.incProduced()
				seq++
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() == nil {
			fail(StageProducer, err)
		}
	}()

	// A worker processes each input fully, gathering its outputs into one batch.
	// Its time splits into idle (waiting for input), busy (Process) and blocked
	// (waiting for the consumer) — the signals that localize the bottleneck.
	var workWG sync.WaitGroup
	worker := func(idx int, input <-chan seqItem[I]) {
		defer workWG.Done()
		wp := pr.wp(idx)
		for {
			var ti time.Time
			if observing {
				ti = time.Now()
			}
			it, ok := <-input
			if !ok {
				return
			}
			wp.addIdle(ti)
			if mi != nil {
				mi.observeIdle(time.Since(ti))
			}

			var tb time.Time
			if observing {
				tb = time.Now()
			}
			var outs []O
			err := proc.Process(ctx, it.val, func(o O) error {
				outs = append(outs, o)
				return nil
			})
			wp.addBusy(tb)
			wp.addEmitted(len(outs))
			if mi != nil {
				mi.observeProcess(time.Since(tb), len(outs))
			}
			if err != nil {
				if ctx.Err() == nil {
					fail(StageProcessor, err)
				}
				return
			}

			var tw time.Time
			if observing {
				tw = time.Now()
			}
			select {
			case done <- batch[O]{seq: it.seq, outs: outs}:
				wp.addBlocked(tw)
				if mi != nil {
					mi.observeBlocked(time.Since(tw))
				}
			case <-ctx.Done():
				return
			}
		}
	}

	if partition == nil {
		// Shared channel: work-stealing across the pool.
		for i := 0; i < cfg.workers; i++ {
			workWG.Add(1)
			go worker(i, in)
		}
	} else {
		// Per-worker channels: route by key so a key is never processed
		// concurrently and keeps its order.
		ins := make([]chan seqItem[I], cfg.workers)
		for w := range ins {
			ins[w] = make(chan seqItem[I], cfg.prefetch)
			workWG.Add(1)
			go worker(w, ins[w])
		}
		go func() {
			defer func() {
				for _, ch := range ins {
					close(ch)
				}
			}()
			for it := range in {
				w := int(partition(it.val) % uint64(cfg.workers))
				select {
				case ins[w] <- it:
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

	// Collector: the single goroutine that calls the consumer.
	stopped := false
	deliver := func(o O) {
		if stopped {
			return
		}
		t := pr.now()
		err := c.Consume(ctx, o)
		pr.consumeSince(t)
		pr.incConsumed()
		mi.incConsumed()
		if err != nil {
			fail(StageConsumer, err)
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

	if cfg.observe != nil {
		*cfg.observe = pr.report(cfg.workers, time.Since(start))
	}

	// A cancelled caller must not get a clean nil back. The producer and the
	// workers bail out quietly on ctx.Done() by design — nobody records that as
	// a failure — so without this the caller cannot tell a truncated run from a
	// finished one. A genuine pipeline error still wins: it is already in
	// firstErr by the time anything cancels.
	if firstErr == nil {
		return parent.Err()
	}
	return firstErr
}
