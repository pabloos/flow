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

// key folds name and attrs into one instrument identity: attributed and
// unattributed instruments of the same name are distinct series.
func key(name string, attrs []flow.Attr) string {
	k := name
	for _, a := range attrs {
		k += "|" + a.Key + "=" + a.Value
	}
	return k
}

// labels turns flow attrs into Prometheus const labels, baked into the
// instrument at creation so recording stays allocation-free.
func labels(attrs []flow.Attr) prometheus.Labels {
	if len(attrs) == 0 {
		return nil
	}
	l := make(prometheus.Labels, len(attrs))
	for _, a := range attrs {
		l[a.Key] = a.Value
	}
	return l
}

func (m *meter) Counter(name string, attrs ...flow.Attr) flow.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(name, attrs)
	if c, ok := m.counters[k]; ok {
		return c
	}
	c := counter{m.factory.NewCounter(prometheus.CounterOpts{
		Namespace:   "flow",
		Name:        name + "_total",
		Help:        "flow pipeline " + name + " count",
		ConstLabels: labels(attrs),
	})}
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
	h := histogram{m.factory.NewHistogram(prometheus.HistogramOpts{
		Namespace:   "flow",
		Name:        name + "_seconds",
		Help:        "flow pipeline " + name + " in seconds",
		Buckets:     prometheus.DefBuckets,
		ConstLabels: labels(attrs),
	})}
	m.histograms[k] = h
	return h
}

type counter struct{ c prometheus.Counter }

func (c counter) Add(n int64) { c.c.Add(float64(n)) }

type histogram struct{ h prometheus.Histogram }

func (h histogram) Record(d time.Duration) { h.h.Observe(d.Seconds()) }
