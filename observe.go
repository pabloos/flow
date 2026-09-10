package flow

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Stage identifies a pipeline stage.
type Stage int

const (
	StageProducer Stage = iota
	StageProcessor
	StageConsumer
)

func (s Stage) String() string {
	switch s {
	case StageProducer:
		return "producer"
	case StageProcessor:
		return "processor"
	case StageConsumer:
		return "consumer"
	default:
		return "unknown"
	}
}

// Report is a snapshot of a run's performance, filled in when Observe is set.
// Timings for the processor are summed across all workers.
type Report struct {
	Wall    time.Duration
	Workers int

	Produced  int64 // values emitted by the producer
	Processed int64 // inputs handled by the pool
	Emitted   int64 // outputs produced by the pool (fan-out with FlatMap)
	Consumed  int64 // values delivered to the consumer

	ProducerBlocked time.Duration // producer time blocked emitting (downstream full)
	WorkerBusy      time.Duration // time inside Process
	WorkerIdle      time.Duration // time waiting for input (producer slow)
	WorkerBlocked   time.Duration // time blocked delivering (consumer slow)
	ConsumerBusy    time.Duration // time inside Consume

	Bottleneck Stage
	Advice     string
}

// Util is the fraction of available worker-time spent doing work (0..1).
func (r *Report) Util() float64 {
	total := time.Duration(r.Workers) * r.Wall
	if total <= 0 {
		return 0
	}
	return float64(r.WorkerBusy) / float64(total)
}

func (r *Report) String() string {
	round := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
	var b strings.Builder
	fmt.Fprintf(&b, "flow report — wall %s · %d workers\n", round(r.Wall), r.Workers)
	fmt.Fprintf(&b, "  producer   %d items   blocked %s\n", r.Produced, round(r.ProducerBlocked))
	fmt.Fprintf(&b, "  processor  %d in / %d out   busy %s  idle %s  blocked %s  · %.0f%% util\n",
		r.Processed, r.Emitted, round(r.WorkerBusy), round(r.WorkerIdle), round(r.WorkerBlocked), r.Util()*100)
	fmt.Fprintf(&b, "  consumer   %d items   busy %s\n", r.Consumed, round(r.ConsumerBusy))
	fmt.Fprintf(&b, "  → bottleneck: %s\n    %s\n", strings.ToUpper(r.Bottleneck.String()), r.Advice)
	return b.String()
}

// probe accumulates timings during a run. It is nil unless Observe is set, and
// every method is a no-op on a nil probe, so the non-observed path pays only a
// branch (no time.Now calls).
type probe struct {
	prodBlocked atomic.Int64
	busy        atomic.Int64
	idle        atomic.Int64
	blocked     atomic.Int64
	consume     atomic.Int64
	produced    atomic.Int64
	processed   atomic.Int64
	emitted     atomic.Int64
	consumed    atomic.Int64
}

func (p *probe) now() time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Now()
}

func (p *probe) prodBlockedSince(t time.Time) {
	if p != nil {
		p.prodBlocked.Add(int64(time.Since(t)))
	}
}
func (p *probe) idleSince(t time.Time) {
	if p != nil {
		p.idle.Add(int64(time.Since(t)))
	}
}
func (p *probe) busySince(t time.Time) {
	if p != nil {
		p.busy.Add(int64(time.Since(t)))
	}
}
func (p *probe) blockedSince(t time.Time) {
	if p != nil {
		p.blocked.Add(int64(time.Since(t)))
	}
}
func (p *probe) consumeSince(t time.Time) {
	if p != nil {
		p.consume.Add(int64(time.Since(t)))
	}
}
func (p *probe) incProduced() {
	if p != nil {
		p.produced.Add(1)
	}
}
func (p *probe) incProcessed() {
	if p != nil {
		p.processed.Add(1)
	}
}
func (p *probe) addEmitted(n int) {
	if p != nil {
		p.emitted.Add(int64(n))
	}
}
func (p *probe) incConsumed() {
	if p != nil {
		p.consumed.Add(1)
	}
}

func (p *probe) report(workers int, wall time.Duration) Report {
	r := Report{
		Wall:            wall,
		Workers:         workers,
		Produced:        p.produced.Load(),
		Processed:       p.processed.Load(),
		Emitted:         p.emitted.Load(),
		Consumed:        p.consumed.Load(),
		ProducerBlocked: time.Duration(p.prodBlocked.Load()),
		WorkerBusy:      time.Duration(p.busy.Load()),
		WorkerIdle:      time.Duration(p.idle.Load()),
		WorkerBlocked:   time.Duration(p.blocked.Load()),
		ConsumerBusy:    time.Duration(p.consume.Load()),
	}

	// Triangulate: a worker's time is idle (upstream slow), busy (itself) or
	// blocked (downstream slow). The largest bucket names the bottleneck.
	switch {
	case r.WorkerIdle >= r.WorkerBusy && r.WorkerIdle >= r.WorkerBlocked:
		r.Bottleneck = StageProducer
		r.Advice = "producer-bound: the source can't feed the pool fast enough"
	case r.WorkerBlocked >= r.WorkerBusy && r.WorkerBlocked >= r.WorkerIdle:
		r.Bottleneck = StageConsumer
		r.Advice = "consumer-bound: the sink is the limit — speed it up or batch writes"
	default:
		r.Bottleneck = StageProcessor
		r.Advice = "processor-bound: raise Workers() or optimize Process"
	}
	return r
}
