package flow_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

type ev struct {
	key string
	n   int
}

// Values of the same key must stay in order, even with a pool and jitter.
func TestPartitionKeepsPerKeyOrder(t *testing.T) {
	keys := []string{"a", "b", "c"}
	counts := map[string]int{}
	var input []ev
	for i := 0; i < 60; i++ {
		k := keys[i%3]
		input = append(input, ev{key: k, n: counts[k]})
		counts[k]++
	}

	proc := flow.ProcessorFunc[ev, ev](func(ctx context.Context, e ev, emit func(ev) error) error {
		time.Sleep(time.Millisecond) // jitter: would reorder without partitioning
		return emit(e)
	})

	var got []ev
	err := flow.RunPartitioned(context.Background(), flow.Slice(input...), proc, flow.Into(&got),
		func(e ev) string { return e.key },
		flow.Workers(4), flow.Prefetch(8))
	if err != nil {
		t.Fatal(err)
	}

	last := map[string]int{"a": -1, "b": -1, "c": -1}
	seen := map[string]int{}
	for _, e := range got {
		if last[e.key] != -1 && e.n != last[e.key]+1 {
			t.Fatalf("key %q out of order: saw %d after %d", e.key, e.n, last[e.key])
		}
		last[e.key] = e.n
		seen[e.key]++
	}
	for _, k := range keys {
		if seen[k] != 20 {
			t.Fatalf("key %q: got %d values, want 20", k, seen[k])
		}
	}
}

// Partition routing combined with Ordered still restores the global order.
func TestPartitionedOrderedGlobal(t *testing.T) {
	double := flow.Map(func(n int) int { return n * 2 })

	nums := make([]int, 0, 20)
	want := make([]int, 0, 20)
	for i := 1; i <= 20; i++ {
		nums = append(nums, i)
		want = append(want, i*2)
	}

	var got []int
	err := flow.RunPartitioned(context.Background(), flow.Slice(nums...), double, flow.Into(&got),
		func(n int) int { return n % 4 },
		flow.Workers(4), flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Prefetch must not change results, only scheduling.
func TestPrefetchCorrectness(t *testing.T) {
	double := flow.Map(func(n int) int { return n * 2 })

	nums := make([]int, 0, 20)
	want := make([]int, 0, 20)
	for i := 1; i <= 20; i++ {
		nums = append(nums, i)
		want = append(want, i*2)
	}

	var got []int
	err := flow.Run(context.Background(), flow.Slice(nums...), double, flow.Into(&got),
		flow.Workers(3), flow.Ordered(), flow.Prefetch(16))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
