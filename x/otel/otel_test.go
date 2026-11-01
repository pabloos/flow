package otel_test

import (
	"context"
	"testing"

	"github.com/pabloos/flow"
	flowotel "github.com/pabloos/flow/x/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMeterRecordsToReader(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := flowotel.NewMeter(provider.Meter("flow-test"))

	err := flow.Run(context.Background(),
		flow.Slice(1, 2, 3, 4, 5),
		flow.FlatMap(func(n int) []int { return []int{n, -n} }), // 1 -> 2
		flow.Into(new([]int)),
		flow.Workers(2), flow.WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}

	rm := collect(t, reader)
	if got := counterValue(t, rm, "flow.produced"); got != 5 {
		t.Errorf("flow.produced = %d, want 5", got)
	}
	if got := counterValue(t, rm, "flow.processed"); got != 5 {
		t.Errorf("flow.processed = %d, want 5", got)
	}
	if got := counterValue(t, rm, "flow.emitted"); got != 10 {
		t.Errorf("flow.emitted = %d, want 10", got)
	}
	if got := counterValue(t, rm, "flow.consumed"); got != 10 {
		t.Errorf("flow.consumed = %d, want 10", got)
	}
	if got := histogramCount(t, rm, "flow.process_latency"); got != 5 {
		t.Errorf("process_latency sample count = %d, want 5", got)
	}
	// Since flow v0.6.0 the Meter also streams the worker idle/blocked split:
	// one sample per item received / delivered.
	if got := histogramCount(t, rm, "flow.worker_idle"); got != 5 {
		t.Errorf("worker_idle sample count = %d, want 5", got)
	}
	if got := histogramCount(t, rm, "flow.worker_blocked"); got != 5 {
		t.Errorf("worker_blocked sample count = %d, want 5", got)
	}
}

// Reusing a Meter across runs must not re-create instruments or lose counts.
func TestMeterReuseAcrossRuns(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := flowotel.NewMeter(provider.Meter("flow-test"))
	double := flow.Map(func(n int) int { return n * 2 })

	for i := 0; i < 3; i++ {
		if err := flow.Run(context.Background(), flow.Slice(1, 2), double,
			flow.Into(new([]int)), flow.WithMeter(m)); err != nil {
			t.Fatal(err)
		}
	}
	if got := counterValue(t, collect(t, reader), "flow.consumed"); got != 6 {
		t.Errorf("flow.consumed = %d, want 6 (2 x 3 runs)", got)
	}
}

func collect(t *testing.T, r sdkmetric.Reader) *metricdata.ResourceMetrics {
	t.Helper()
	rm := new(metricdata.ResourceMetrics)
	if err := r.Collect(context.Background(), rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func counterValue(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	sum, ok := findMetric(t, rm, name).Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is not an Int64 Sum", name)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func histogramCount(t *testing.T, rm *metricdata.ResourceMetrics, name string) uint64 {
	t.Helper()
	h, ok := findMetric(t, rm, name).Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q is not a Float64 Histogram", name)
	}
	var count uint64
	for _, dp := range h.DataPoints {
		count += dp.Count
	}
	return count
}

func findMetric(t *testing.T, rm *metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}
