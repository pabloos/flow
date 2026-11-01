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

func (m *meter) Counter(name string) flow.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		return c
	}
	inst, err := m.m.Int64Counter("flow."+name,
		metric.WithDescription("flow pipeline "+name+" count"))
	if err != nil {
		// A failed instrument means a misconfigured Meter, not a runtime
		// condition: surface it at setup rather than silently dropping metrics.
		panic("flow/x/otel: " + err.Error())
	}
	c := counter{inst}
	m.counters[name] = c
	return c
}

func (m *meter) Histogram(name string) flow.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.histograms[name]; ok {
		return h
	}
	inst, err := m.m.Float64Histogram("flow."+name,
		metric.WithDescription("flow pipeline "+name+" in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		panic("flow/x/otel: " + err.Error())
	}
	h := histogram{inst}
	m.histograms[name] = h
	return h
}

type counter struct{ c metric.Int64Counter }

func (c counter) Add(n int64) { c.c.Add(context.Background(), n) }

type histogram struct{ h metric.Float64Histogram }

func (h histogram) Record(d time.Duration) { h.h.Record(context.Background(), d.Seconds()) }
