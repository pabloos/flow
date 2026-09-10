package flow_test

import (
	"context"
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
