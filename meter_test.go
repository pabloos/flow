package flow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

// fakeMeter is an in-memory Meter: it proves the contract is implementable and
// lets us assert what the engine records — no external dependency needed.
// Instruments are keyed by name *and* attributes, the way a real exporter must
// key them, so attrs are part of an instrument's identity.
type fakeMeter struct {
	mu       sync.Mutex
	counters map[string]int64
	samples  map[string]int
	asked    map[string][]flow.Attr // instrument name -> attrs it was created with
}

func newFakeMeter() *fakeMeter {
	return &fakeMeter{
		counters: map[string]int64{},
		samples:  map[string]int{},
		asked:    map[string][]flow.Attr{},
	}
}

// A Meter that implements nothing but Counter and Histogram must satisfy the
// interface — the stability promise in meter.go, enforced at compile time.
var _ flow.Meter = (*fakeMeter)(nil)

// key folds name and attrs into one instrument identity.
func key(name string, attrs []flow.Attr) string {
	k := name
	for _, a := range attrs {
		k += "|" + a.Key + "=" + a.Value
	}
	return k
}

func (m *fakeMeter) Counter(name string, attrs ...flow.Attr) flow.Counter {
	m.note(name, attrs)
	return fakeCounter{m, key(name, attrs)}
}

func (m *fakeMeter) Histogram(name string, attrs ...flow.Attr) flow.Histogram {
	m.note(name, attrs)
	return fakeHistogram{m, key(name, attrs)}
}

func (m *fakeMeter) note(name string, attrs []flow.Attr) {
	m.mu.Lock()
	m.asked[name] = attrs
	m.mu.Unlock()
}

func (m *fakeMeter) count(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}
func (m *fakeMeter) records(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.samples[name]
}
func (m *fakeMeter) attrsFor(name string) []flow.Attr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.asked[name]
}

type fakeCounter struct {
	m    *fakeMeter
	name string
}

func (c fakeCounter) Add(n int64) {
	c.m.mu.Lock()
	c.m.counters[c.name] += n
	c.m.mu.Unlock()
}

type fakeHistogram struct {
	m    *fakeMeter
	name string
}

func (h fakeHistogram) Record(time.Duration) {
	h.m.mu.Lock()
	h.m.samples[h.name]++
	h.m.mu.Unlock()
}

func TestWithMeterRecords(t *testing.T) {
	m := newFakeMeter()
	n := 100
	err := flow.Run(context.Background(), flow.Slice(rangeN(n)...),
		flow.FlatMap(func(x int) []int { return []int{x, x} }), // 1 -> 2
		flow.Into(new([]int)),
		flow.Workers(4), flow.WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}

	if got := m.count("produced"); got != int64(n) {
		t.Fatalf("produced = %d, want %d", got, n)
	}
	if got := m.count("processed"); got != int64(n) {
		t.Fatalf("processed = %d, want %d", got, n)
	}
	if got := m.count("emitted"); got != int64(2*n) {
		t.Fatalf("emitted = %d, want %d", got, 2*n)
	}
	if got := m.count("consumed"); got != int64(2*n) {
		t.Fatalf("consumed = %d, want %d", got, 2*n)
	}
	if got := m.records("process_latency"); got != n {
		t.Fatalf("process_latency records = %d, want %d", got, n)
	}
	// worker_idle is recorded once per item received, worker_blocked once per
	// batch delivered downstream — so both see exactly n intervals. These are
	// recorded with no Observe set, proving the Meter times them on its own.
	if got := m.records("worker_idle"); got != n {
		t.Fatalf("worker_idle records = %d, want %d", got, n)
	}
	if got := m.records("worker_blocked"); got != n {
		t.Fatalf("worker_blocked records = %d, want %d", got, n)
	}
}

// The engine's own instruments are pipeline-wide aggregates, so it asks for
// them with no attributes. Pinned here so a stray attr — which would split a
// series and break Prometheus' stable-label-dimension rule — fails loudly.
func TestEngineAsksForAggregateInstrumentsWithoutAttrs(t *testing.T) {
	m := newFakeMeter()
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3),
		flow.Map(func(n int) int { return n }), flow.Into(new([]int)),
		flow.WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"produced", "processed", "emitted", "consumed",
		"process_latency", "worker_idle", "worker_blocked",
	} {
		if got := m.attrsFor(name); len(got) != 0 {
			t.Errorf("%s created with attrs %v, want none", name, got)
		}
	}
}

// Attributes are part of an instrument's identity: same name, different attrs
// means different instruments. This is the contract exporters must honour when
// flow starts labelling (e.g. per-worker breakdowns).
func TestMeterAttrsAreInstrumentIdentity(t *testing.T) {
	m := newFakeMeter()
	w0 := m.Counter("processed", flow.Attr{Key: "worker", Value: "0"})
	w1 := m.Counter("processed", flow.Attr{Key: "worker", Value: "1"})
	plain := m.Counter("processed")

	w0.Add(5)
	w1.Add(7)
	plain.Add(1)

	if got := m.count("processed|worker=0"); got != 5 {
		t.Errorf("worker=0 count = %d, want 5", got)
	}
	if got := m.count("processed|worker=1"); got != 7 {
		t.Errorf("worker=1 count = %d, want 7", got)
	}
	if got := m.count("processed"); got != 1 {
		t.Errorf("unattributed count = %d, want 1", got)
	}
}

// A Meter and a Report can be collected in the same run.
func TestWithMeterAndObserveCoexist(t *testing.T) {
	m := newFakeMeter()
	var rep flow.Report
	err := flow.Run(context.Background(), flow.Slice(rangeN(50)...),
		flow.Map(func(x int) int { return x * 2 }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.WithMeter(m), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if m.count("consumed") != 50 || rep.Consumed != 50 {
		t.Fatalf("meter consumed=%d, report consumed=%d, want 50/50", m.count("consumed"), rep.Consumed)
	}
}
