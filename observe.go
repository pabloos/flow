package flow

import (
	"fmt"
	"sort"
	"strings"
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

// StageLatency holds the per-item latency distribution of Process. Percentiles
// are computed from a bounded reservoir sample, so they are approximate; Max is
// exact.
type StageLatency struct {
	P50, P95, P99, Max time.Duration
}

// WorkerStat is one worker's share of the work, for spotting imbalance.
type WorkerStat struct {
	Processed           int64
	Busy, Idle, Blocked time.Duration
}

// Report is a snapshot of a run's performance, filled in when Observe is set.
// Aggregate processor timings are summed across all workers; PerWorker holds
// the per-worker breakdown.
//
// Experimental: this type may change in a future v0.x release.
type Report struct {
	Wall    time.Duration
	Workers int

	Produced  int64 // values emitted by the producer
	Processed int64 // inputs handled by the pool
	Emitted   int64 // outputs produced by the pool (fan-out with FlatMap)
	Consumed  int64 // values delivered to the consumer

	ProducerBlocked time.Duration // producer time blocked emitting (downstream full)
	WorkerBusy      time.Duration // time inside Process (summed)
	WorkerIdle      time.Duration // time waiting for input (producer slow)
	WorkerBlocked   time.Duration // time blocked delivering (consumer slow)
	ConsumerBusy    time.Duration // time inside Consume

	ProcessLatency StageLatency // per-item Process latency distribution
	PerWorker      []WorkerStat // per-worker breakdown

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
	ms := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
	us := func(d time.Duration) time.Duration { return d.Round(time.Microsecond) }
	var b strings.Builder
	fmt.Fprintf(&b, "flow report — wall %s · %d workers\n", ms(r.Wall), r.Workers)
	fmt.Fprintf(&b, "  producer   %d items   blocked %s\n", r.Produced, ms(r.ProducerBlocked))
	fmt.Fprintf(&b, "  processor  %d in / %d out   busy %s  idle %s  blocked %s  · %.0f%% util\n",
		r.Processed, r.Emitted, ms(r.WorkerBusy), ms(r.WorkerIdle), ms(r.WorkerBlocked), r.Util()*100)
	l := r.ProcessLatency
	fmt.Fprintf(&b, "             latency  p50 %s  p95 %s  p99 %s  max %s\n",
		us(l.P50), us(l.P95), us(l.P99), us(l.Max))
	if len(r.PerWorker) > 1 {
		lo, hi := r.PerWorker[0].Busy, r.PerWorker[0].Busy
		for _, w := range r.PerWorker {
			if w.Busy < lo {
				lo = w.Busy
			}
			if w.Busy > hi {
				hi = w.Busy
			}
		}
		fmt.Fprintf(&b, "             balance  busy %s–%s across %d workers\n", ms(lo), ms(hi), len(r.PerWorker))
	}
	fmt.Fprintf(&b, "  consumer   %d items   busy %s\n", r.Consumed, ms(r.ConsumerBusy))
	fmt.Fprintf(&b, "  → bottleneck: %s\n    %s\n", strings.ToUpper(r.Bottleneck.String()), r.Advice)
	return b.String()
}

// reservoirCap bounds the per-worker latency sample.
const reservoirCap = 4096

// workerProbe is written only by its own worker goroutine, so it needs no
// synchronization; it is read after the workers have finished.
type workerProbe struct {
	busy, idle, blocked int64 // ns
	emitted             int64
	count               int64
	maxProc             int64
	samples             []int64 // reservoir of Process durations (ns)
	rng                 uint64
}

func (w *workerProbe) now() time.Time {
	if w == nil {
		return time.Time{}
	}
	return time.Now()
}
func (w *workerProbe) addIdle(t time.Time) {
	if w != nil {
		w.idle += int64(time.Since(t))
	}
}
func (w *workerProbe) addBlocked(t time.Time) {
	if w != nil {
		w.blocked += int64(time.Since(t))
	}
}
func (w *workerProbe) addEmitted(n int) {
	if w != nil {
		w.emitted += int64(n)
	}
}
func (w *workerProbe) addBusy(t time.Time) {
	if w == nil {
		return
	}
	d := int64(time.Since(t))
	w.busy += d
	w.count++
	if d > w.maxProc {
		w.maxProc = d
	}
	if len(w.samples) < reservoirCap {
		w.samples = append(w.samples, d)
		return
	}
	if j := w.rand() % uint64(w.count); j < reservoirCap {
		w.samples[j] = d
	}
}

// rand is a worker-local xorshift64, used for reservoir replacement.
func (w *workerProbe) rand() uint64 {
	x := w.rng
	if x == 0 {
		x = 0x9E3779B97F4A7C15
	}
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	w.rng = x
	return x
}

// probe holds the per-worker probes plus the single-goroutine producer and
// consumer counters. It is nil unless Observe is set; every method is a no-op
// on a nil probe, so the non-observed path makes no time.Now calls.
type probe struct {
	workers     []workerProbe
	prodBlocked int64
	produced    int64
	consume     int64
	consumed    int64
}

func newProbe(workers int) *probe {
	return &probe{workers: make([]workerProbe, workers)}
}

func (p *probe) now() time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Now()
}
func (p *probe) wp(i int) *workerProbe {
	if p == nil {
		return nil
	}
	return &p.workers[i]
}
func (p *probe) prodBlockedSince(t time.Time) {
	if p != nil {
		p.prodBlocked += int64(time.Since(t))
	}
}
func (p *probe) incProduced() {
	if p != nil {
		p.produced++
	}
}
func (p *probe) consumeSince(t time.Time) {
	if p != nil {
		p.consume += int64(time.Since(t))
	}
}
func (p *probe) incConsumed() {
	if p != nil {
		p.consumed++
	}
}

func (p *probe) report(workers int, wall time.Duration) Report {
	r := Report{
		Wall:            wall,
		Workers:         workers,
		Produced:        p.produced,
		Consumed:        p.consumed,
		ProducerBlocked: time.Duration(p.prodBlocked),
		ConsumerBusy:    time.Duration(p.consume),
		PerWorker:       make([]WorkerStat, len(p.workers)),
	}

	var samples []int64
	var maxProc int64
	for i := range p.workers {
		w := &p.workers[i]
		r.WorkerBusy += time.Duration(w.busy)
		r.WorkerIdle += time.Duration(w.idle)
		r.WorkerBlocked += time.Duration(w.blocked)
		r.Processed += w.count
		r.Emitted += w.emitted
		if w.maxProc > maxProc {
			maxProc = w.maxProc
		}
		samples = append(samples, w.samples...)
		r.PerWorker[i] = WorkerStat{
			Processed: w.count,
			Busy:      time.Duration(w.busy),
			Idle:      time.Duration(w.idle),
			Blocked:   time.Duration(w.blocked),
		}
	}
	r.ProcessLatency = percentiles(samples, maxProc)

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

func percentiles(samples []int64, max int64) StageLatency {
	if len(samples) == 0 {
		return StageLatency{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	at := func(q float64) time.Duration {
		i := int(q * float64(len(samples)))
		if i >= len(samples) {
			i = len(samples) - 1
		}
		return time.Duration(samples[i])
	}
	return StageLatency{P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: time.Duration(max)}
}
