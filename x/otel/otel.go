// Package otel adapts flow's Meter to the OpenTelemetry metrics API, so a
// pipeline's measurements flow to whatever backend your OTel pipeline is wired
// to (OTLP, the Prometheus bridge, stdout) alongside the rest of your
// application's telemetry. It is a separate module so the flow core stays
// zero-dependency: only importers of this package pull in OpenTelemetry.
package otel

import (
	"context"
	"sync"
	"time"

	"github.com/pabloos/flow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// latencyBuckets mirrors Prometheus' default buckets so the two exporters
// report comparable process_latency distributions.
var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

// NewMeter returns a flow.Meter backed by the OpenTelemetry metric.Meter m,
// which you obtain from your MeterProvider (e.g. provider.Meter("flow")).
// Counters are created as Int64 counters named flow.<name>, and the
// process_latency histogram as a Float64 histogram flow.<name> measured in
// seconds. Instruments are memoized, so the same Meter can be reused across
// runs without re-creating them.
func NewMeter(m metric.Meter) flow.Meter {
	return &meter{
		m:          m,
		counters:   make(map[string]flow.Counter),
		histograms: make(map[string]flow.Histogram),
	}
}

type meter struct {
	m          metric.Meter
	mu         sync.Mutex
	counters   map[string]flow.Counter
	histograms map[string]flow.Histogram
}

// key folds name and attrs into one instrument identity: attributed and
// unattributed instruments of the same name are distinct series.
func key(name string, attrs []flow.Attr) string {
	k := name
	for _, a := range attrs {
		k += "|" + a.Key + "=" + a.Value
	}
	return k
}

// kvs converts flow attrs to OpenTelemetry attributes. OTel attaches them per
// measurement, so the option is precomputed once here and reused on every
// Add/Record, keeping the recording path allocation-free.
func kvs(attrs []flow.Attr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		out[i] = attribute.String(a.Key, a.Value)
	}
	return out
}

func (m *meter) Counter(name string, attrs ...flow.Attr) flow.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(name, attrs)
	if c, ok := m.counters[k]; ok {
		return c
	}
	inst, err := m.m.Int64Counter("flow."+name,
		metric.WithDescription("flow pipeline "+name+" count"))
	if err != nil {
		// A failed instrument means a misconfigured Meter, not a runtime
		// condition: surface it at setup rather than silently dropping metrics.
		panic("flow/x/otel: " + err.Error())
	}
	c := counter{c: inst}
	if set := kvs(attrs); len(set) > 0 {
		c.opts = []metric.AddOption{metric.WithAttributes(set...)}
	}
	m.counters[k] = c
	return c
}

func (m *meter) Histogram(name string, attrs ...flow.Attr) flow.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(name, attrs)
	if h, ok := m.histograms[k]; ok {
		return h
	}
	inst, err := m.m.Float64Histogram("flow."+name,
		metric.WithDescription("flow pipeline "+name+" in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		panic("flow/x/otel: " + err.Error())
	}
	h := histogram{h: inst}
	if set := kvs(attrs); len(set) > 0 {
		h.opts = []metric.RecordOption{metric.WithAttributes(set...)}
	}
	m.histograms[k] = h
	return h
}

type counter struct {
	c    metric.Int64Counter
	opts []metric.AddOption
}

func (c counter) Add(n int64) { c.c.Add(context.Background(), n, c.opts...) }

type histogram struct {
	h    metric.Float64Histogram
	opts []metric.RecordOption
}

func (h histogram) Record(d time.Duration) {
	h.h.Record(context.Background(), d.Seconds(), h.opts...)
}
