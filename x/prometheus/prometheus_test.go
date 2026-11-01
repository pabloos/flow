package prometheus_test

import (
	"context"
	"testing"

	"github.com/pabloos/flow"
	flowprom "github.com/pabloos/flow/x/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMeterRecordsToRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := flowprom.NewMeter(reg)

	err := flow.Run(context.Background(),
		flow.Slice(1, 2, 3, 4, 5),
		flow.FlatMap(func(n int) []int { return []int{n, -n} }), // 1 -> 2
		flow.Into(new([]int)),
		flow.Workers(2), flow.WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}

	if got := counterValue(t, reg, "flow_produced_total"); got != 5 {
		t.Errorf("flow_produced_total = %v, want 5", got)
	}
	if got := counterValue(t, reg, "flow_processed_total"); got != 5 {
		t.Errorf("flow_processed_total = %v, want 5", got)
	}
	if got := counterValue(t, reg, "flow_emitted_total"); got != 10 {
		t.Errorf("flow_emitted_total = %v, want 10", got)
	}
	if got := counterValue(t, reg, "flow_consumed_total"); got != 10 {
		t.Errorf("flow_consumed_total = %v, want 10", got)
	}
	if got := histogramCount(t, reg, "flow_process_latency_seconds"); got != 5 {
		t.Errorf("process_latency sample count = %d, want 5", got)
	}
	// Since flow v0.6.0 the Meter also streams the worker idle/blocked split:
	// one sample per item received / delivered.
	if got := histogramCount(t, reg, "flow_worker_idle_seconds"); got != 5 {
		t.Errorf("worker_idle sample count = %d, want 5", got)
	}
	if got := histogramCount(t, reg, "flow_worker_blocked_seconds"); got != 5 {
		t.Errorf("worker_blocked sample count = %d, want 5", got)
	}
}

// Reusing a Meter across runs must not panic on duplicate registration.
func TestMeterReuseAcrossRuns(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := flowprom.NewMeter(reg)
	double := flow.Map(func(n int) int { return n * 2 })

	for i := 0; i < 3; i++ {
		if err := flow.Run(context.Background(), flow.Slice(1, 2), double,
			flow.Into(new([]int)), flow.WithMeter(m)); err != nil {
			t.Fatal(err)
		}
	}
	if got := counterValue(t, reg, "flow_consumed_total"); got != 6 {
		t.Errorf("flow_consumed_total = %v, want 6 (2 x 3 runs)", got)
	}
}

// flow.Attr becomes a Prometheus const label, and attrs are part of an
// instrument's identity: same name, different attrs means separate series.
func TestAttrsBecomeConstLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := flowprom.NewMeter(reg)

	m.Counter("processed", flow.Attr{Key: "worker", Value: "0"}).Add(5)
	m.Counter("processed", flow.Attr{Key: "worker", Value: "1"}).Add(7)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "flow_processed_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "worker" {
					got[l.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	if len(got) != 2 || got["0"] != 5 || got["1"] != 7 {
		t.Fatalf("worker series = %v, want {0:5, 1:7}", got)
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}
