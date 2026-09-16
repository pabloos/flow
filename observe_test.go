package flow_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

func rangeN(n int) []int {
	xs := make([]int, 0, n)
	for i := 0; i < n; i++ {
		xs = append(xs, i)
	}
	return xs
}

func TestObserveCounts(t *testing.T) {
	var rep flow.Report
	n := 100
	err := flow.Run(context.Background(), flow.Slice(rangeN(n)...),
		flow.FlatMap(func(x int) []int { return []int{x, x} }), // 1 -> 2
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Produced != int64(n) || rep.Processed != int64(n) {
		t.Fatalf("in counts: produced=%d processed=%d want %d", rep.Produced, rep.Processed, n)
	}
	if rep.Emitted != int64(2*n) || rep.Consumed != int64(2*n) {
		t.Fatalf("out counts: emitted=%d consumed=%d want %d", rep.Emitted, rep.Consumed, 2*n)
	}
	if rep.Workers != 4 {
		t.Fatalf("workers = %d, want 4", rep.Workers)
	}
}

// A slow consumer makes workers spend their time blocked on delivery.
func TestBottleneckConsumer(t *testing.T) {
	var rep flow.Report
	err := flow.Run(context.Background(), flow.Slice(rangeN(50)...),
		flow.Map(func(x int) int { return x }),
		flow.Each(func(int) error { time.Sleep(time.Millisecond); return nil }),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bottleneck != flow.StageConsumer {
		t.Fatalf("want consumer, got %s\n%s", rep.Bottleneck, &rep)
	}
}

// A slow processor keeps the workers busy.
func TestBottleneckProcessor(t *testing.T) {
	var rep flow.Report
	err := flow.Run(context.Background(), flow.Slice(rangeN(50)...),
		flow.Map(func(x int) int { time.Sleep(time.Millisecond); return x }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bottleneck != flow.StageProcessor {
		t.Fatalf("want processor, got %s\n%s", rep.Bottleneck, &rep)
	}
}

// A slow producer starves the workers.
func TestBottleneckProducer(t *testing.T) {
	var rep flow.Report
	slow := flow.ProducerFunc[int](func(ctx context.Context, emit func(int) error) error {
		for i := 0; i < 50; i++ {
			time.Sleep(time.Millisecond)
			if err := emit(i); err != nil {
				return err
			}
		}
		return nil
	})
	err := flow.Run(context.Background(), slow,
		flow.Map(func(x int) int { return x }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bottleneck != flow.StageProducer {
		t.Fatalf("want producer, got %s\n%s", rep.Bottleneck, &rep)
	}
}

func TestReportStringAndUtil(t *testing.T) {
	r := flow.Report{
		Wall: 100 * time.Millisecond, Workers: 2,
		Produced: 10, Processed: 10, Emitted: 10, Consumed: 10,
		WorkerBusy: 180 * time.Millisecond, // 180 / (2*100) = 0.9
		Bottleneck: flow.StageProcessor, Advice: "raise Workers()",
	}
	if u := r.Util(); u < 0.89 || u > 0.91 {
		t.Fatalf("util = %v, want ~0.9", u)
	}
	s := r.String()
	for _, want := range []string{"flow report", "2 workers", "processor", "PROCESSOR", "raise Workers()"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() missing %q:\n%s", want, s)
		}
	}
}

func TestObservePercentiles(t *testing.T) {
	var rep flow.Report
	err := flow.Run(context.Background(), flow.Slice(rangeN(200)...),
		flow.Map(func(x int) int { time.Sleep(time.Millisecond); return x }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	l := rep.ProcessLatency
	if !(l.P50 > 0 && l.P95 >= l.P50 && l.P99 >= l.P95 && l.Max >= l.P99) {
		t.Fatalf("percentiles not monotonic: %+v", l)
	}
	// each Process sleeps ~1ms; p50 should be in a generous band
	if l.P50 < 500*time.Microsecond || l.P50 > 25*time.Millisecond {
		t.Fatalf("p50 = %s, want ~1ms", l.P50)
	}
}

func TestPerWorkerBreakdown(t *testing.T) {
	var rep flow.Report
	n := 200
	err := flow.Run(context.Background(), flow.Slice(rangeN(n)...),
		flow.Map(func(x int) int { return x }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.PerWorker) != 4 {
		t.Fatalf("PerWorker len = %d, want 4", len(rep.PerWorker))
	}
	var total int64
	for _, w := range rep.PerWorker {
		total += w.Processed
	}
	if total != int64(n) {
		t.Fatalf("per-worker processed sum = %d, want %d", total, n)
	}
}

func TestObserveReportStringFull(t *testing.T) {
	var rep flow.Report
	err := flow.Run(context.Background(), flow.Slice(rangeN(5000)...), // > reservoir cap
		flow.Map(func(x int) int { return x }),
		flow.Into(new([]int)),
		flow.Workers(4), flow.Observe(&rep))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Processed != 5000 {
		t.Fatalf("processed = %d, want 5000", rep.Processed)
	}
	s := rep.String()
	for _, want := range []string{"latency", "balance", "4 workers"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() missing %q:\n%s", want, s)
		}
	}
}
