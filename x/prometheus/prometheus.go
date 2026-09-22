// Package prometheus adapts flow's Meter to a Prometheus registry, so a
// pipeline's metrics show up alongside the rest of your application's. It is a
// separate module so the flow core stays zero-dependency: only importers of
// this package pull in client_golang.
package prometheus

import (
	"sync"
	"time"

	"github.com/pabloos/flow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// NewMeter returns a flow.Meter that registers its instruments with reg under
// the "flow_" namespace: counters as flow_<name>_total, and the process_latency
// histogram as flow_<name>_seconds. Instruments are memoized, so the same Meter
// can be reused across runs without re-registering.
func NewMeter(reg prometheus.Registerer) flow.Meter {
	return &meter{
		factory:    promauto.With(reg),
		counters:   make(map[string]flow.Counter),
		histograms: make(map[string]flow.Histogram),
	}
}

type meter struct {
	factory    promauto.Factory
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
	c := counter{m.factory.NewCounter(prometheus.CounterOpts{
		Namespace: "flow",
		Name:      name + "_total",
		Help:      "flow pipeline " + name + " count",
	})}
	m.counters[name] = c
	return c
}

func (m *meter) Histogram(name string) flow.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.histograms[name]; ok {
		return h
	}
	h := histogram{m.factory.NewHistogram(prometheus.HistogramOpts{
		Namespace: "flow",
		Name:      name + "_seconds",
		Help:      "flow pipeline " + name + " in seconds",
		Buckets:   prometheus.DefBuckets,
	})}
	m.histograms[name] = h
	return h
}

type counter struct{ c prometheus.Counter }

func (c counter) Add(n int64) { c.c.Add(float64(n)) }

type histogram struct{ h prometheus.Histogram }

func (h histogram) Record(d time.Duration) { h.h.Observe(d.Seconds()) }
