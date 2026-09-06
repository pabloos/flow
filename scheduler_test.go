package flow_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

func TestRoundRobinRoutes(t *testing.T) {
	rr := flow.RoundRobin()
	loads := []int{0, 0, 0}
	got := []int{
		rr.Route(0, loads), rr.Route(1, loads), rr.Route(2, loads),
		rr.Route(3, loads), rr.Route(4, loads),
	}
	if want := []int{0, 1, 2, 0, 1}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestLeastBusyRoutes(t *testing.T) {
	lb := flow.LeastBusy()
	if got := lb.Route(0, []int{3, 1, 2}); got != 1 {
		t.Fatalf("least loaded: got %d want 1", got)
	}
	if got := lb.Route(0, []int{0, 0, 0}); got != 0 {
		t.Fatalf("tie -> lowest index: got %d want 0", got)
	}
	if got := lb.Route(0, []int{5, 5, 2, 5}); got != 2 {
		t.Fatalf("got %d want 2", got)
	}
}

func TestRandomWithinRange(t *testing.T) {
	rnd := flow.Random()
	loads := []int{0, 0, 0, 0}
	for i := 0; i < 200; i++ {
		if got := rnd.Route(uint64(i), loads); got < 0 || got >= len(loads) {
			t.Fatalf("out of range: %d", got)
		}
	}
}

// LeastBusy over buffered channels still yields deterministic ordered results.
func TestFanOutLeastBusyBufferedInOrder(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	worker := flow.Map(ctx, func(n int) (int, error) { return n * 2, nil })
	in := flow.Source(ctx, 1, 2, 3, 4, 5, 6, 7, 8)
	branches := flow.FanOutN(ctx, in, 3, flow.LeastBusy(), worker, flow.WithBuffer(2))
	merged := flow.FanIn(ctx, branches, flow.WithBuffer(4))

	out, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4, 6, 8, 10, 12, 14, 16}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

// One genuinely slow worker must not stall the pipeline: with LeastBusy +
// buffering the distributor routes around it. Correctness (every element
// processed, in order) is deterministic even though the routing is not.
func TestFanOutLeastBusySlowWorkerCompletes(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	fast := flow.Map(ctx, func(n int) (int, error) { return n, nil })
	slow := flow.Map(ctx, func(n int) (int, error) {
		time.Sleep(time.Millisecond)
		return n, nil
	})

	nums := make([]int, 0, 40)
	for i := 1; i <= 40; i++ {
		nums = append(nums, i)
	}
	in := flow.Source(ctx, nums...)
	workers := []flow.Stage[int, int]{slow, fast, fast, fast}
	branches := flow.FanOut(ctx, in, flow.LeastBusy(), workers, flow.WithBuffer(4))
	merged := flow.FanIn(ctx, branches, flow.WithBuffer(8))

	out, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(out, nums) {
		t.Fatalf("mismatch: got %v", out)
	}
}
