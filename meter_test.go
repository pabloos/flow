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
type fakeMeter struct {
	mu       sync.Mutex
	counters map[string]int64
	samples  map[string]int
}

func newFakeMeter() *fakeMeter {
	return &fakeMeter{counters: map[string]int64{}, samples: map[string]int{}}
}

func (m *fakeMeter) Counter(name string) flow.Counter     { return fakeCounter{m, name} }
func (m *fakeMeter) Histogram(name string) flow.Histogram { return fakeHistogram{m, name} }

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
