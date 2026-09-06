package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pabloos/flow"
)

// Heterogeneous fan-out (double vs square) with deterministic InOrder result,
// mirroring the classic pipelines example from the go blog / original repo.
func TestFanOutInOrder(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	double := flow.Map(ctx, func(n int) (int, error) { return n * 2, nil })
	square := flow.Map(ctx, func(n int) (int, error) { return n * n, nil })

	in := flow.Source(ctx, 1, 2, 3)
	branches := flow.FanOut(ctx, in, flow.RoundRobin(), []flow.Stage[int, int]{double, square})
	merged := flow.FanIn(ctx, branches)

	out, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if err != nil {
		t.Fatal(err)
	}
	// pos0: 1 -> double = 2 ; pos1: 2 -> square = 4 ; pos2: 3 -> double = 6
	if want := []int{2, 4, 6}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

// Data parallelism with N identical workers: NoOrder is nondeterministic in
// sequence but must preserve the full multiset.
func TestFanOutNNoOrderSameSet(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	worker := flow.Map(ctx, func(n int) (int, error) { return n * 10, nil })
	in := flow.Source(ctx, 1, 2, 3, 4, 5, 6)
	merged := flow.FanIn(ctx, flow.FanOutN(ctx, in, 3, flow.RoundRobin(), worker))

	out, err := flow.CollectOrdered(ctx, merged, flow.NoOrder)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	if want := []int{10, 20, 30, 40, 50, 60}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestFanOutReverse(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	worker := flow.Map(ctx, func(n int) (int, error) { return n, nil })
	in := flow.Source(ctx, 1, 2, 3, 4)
	merged := flow.FanIn(ctx, flow.FanOutN(ctx, in, 2, flow.RoundRobin(), worker))

	out, err := flow.CollectOrdered(ctx, merged, flow.Reverse)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{4, 3, 2, 1}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestFanOutFailFast(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	boom := errors.New("boom")
	worker := flow.Map(ctx, func(n int) (int, error) {
		if n == 4 {
			return 0, boom
		}
		return n, nil
	})
	in := flow.Source(ctx, 1, 2, 3, 4, 5, 6)
	merged := flow.FanIn(ctx, flow.FanOutN(ctx, in, 2, flow.RoundRobin(), worker))

	_, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

// A Filter before the fan-out leaves gaps in Element.order (dropped elements).
// InOrder must still reconstruct the correct relative sequence.
func TestFanOutInOrderWithFilterGaps(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	even := flow.Filter(ctx, func(n int) (bool, error) { return n%2 == 0, nil })
	worker := flow.Map(ctx, func(n int) (int, error) { return n * 100, nil })

	filtered := even(flow.Source(ctx, 1, 2, 3, 4, 5, 6)) // 2,4,6 with orders 1,3,5
	branches := flow.FanOutN(ctx, filtered, 3, flow.RoundRobin(), worker)
	merged := flow.FanIn(ctx, branches)

	out, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{200, 400, 600}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}
