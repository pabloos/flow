package flow

import "time"

// Meter is flow's streaming-metrics extension point. Implement it in a separate
// module — so the core stays zero-dependency — to forward live measurements to a
// backend such as Prometheus or OpenTelemetry, and enable it with WithMeter.
//
// The engine asks the Meter for its instruments once when a run starts, then
// updates them on the hot path. Instrument methods are called from many worker
// goroutines at once, so implementations must be safe for concurrent use.
//
// Experimental: the observability API may change in a future v0.x release.
type Meter interface {
	Counter(name string) Counter
	Histogram(name string) Histogram
}

// Counter is a monotonically increasing count.
type Counter interface {
	Add(n int64)
}

// Histogram records a distribution of durations.
type Histogram interface {
	Record(d time.Duration)
}

// WithMeter streams live measurements to m during the run: the counters
// "produced", "processed", "emitted" and "consumed", and the histograms
// "process_latency", "worker_idle" and "worker_blocked" — the same busy / idle
// / blocked split that Observe uses to localize the bottleneck, but live and
// per-interval. Opt-in; with no Meter the hot path pays nothing.
func WithMeter(m Meter) Option {
	return func(c *config) { c.meter = m }
}

// meterInstruments is the fixed set of instruments the engine records to. It is
// nil unless WithMeter is set; every method is a no-op on a nil receiver.
type meterInstruments struct {
	produced, processed, emitted, consumed Counter
	procLatency                            Histogram
	workerIdle, workerBlocked              Histogram
}

func newMeterInstruments(m Meter) *meterInstruments {
	return &meterInstruments{
		produced:      m.Counter("produced"),
		processed:     m.Counter("processed"),
		emitted:       m.Counter("emitted"),
		consumed:      m.Counter("consumed"),
		procLatency:   m.Histogram("process_latency"),
		workerIdle:    m.Histogram("worker_idle"),
		workerBlocked: m.Histogram("worker_blocked"),
	}
}

func (m *meterInstruments) incProduced() {
	if m != nil {
		m.produced.Add(1)
	}
}

func (m *meterInstruments) incConsumed() {
	if m != nil {
		m.consumed.Add(1)
	}
}

// observeProcess records one processed input: its output count and Process
// latency.
func (m *meterInstruments) observeProcess(d time.Duration, emitted int) {
	if m == nil {
		return
	}
	m.processed.Add(1)
	if emitted > 0 {
		m.emitted.Add(int64(emitted))
	}
	m.procLatency.Record(d)
}

// observeIdle records one interval a worker spent waiting for input (producer
// too slow). observeBlocked records one interval it spent waiting to deliver
// downstream (consumer too slow).
func (m *meterInstruments) observeIdle(d time.Duration) {
	if m != nil {
		m.workerIdle.Record(d)
	}
}

func (m *meterInstruments) observeBlocked(d time.Duration) {
	if m != nil {
		m.workerBlocked.Record(d)
	}
}
