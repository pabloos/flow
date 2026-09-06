package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pabloos/flow"
)

// Linear FlatMap: children stay contiguous and in order (FIFO).
func TestFlatMapLinear(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	dup := flow.FlatMap(ctx, func(n int) ([]int, error) { return []int{n, n * 10}, nil })

	out, err := flow.Collect(ctx, dup(flow.Source(ctx, 1, 2, 3)))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 10, 2, 20, 3, 30}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

// Returning an empty slice drops the input, like a filter.
func TestFlatMapEmptyDrops(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	evens := flow.FlatMap(ctx, func(n int) ([]int, error) {
		if n%2 == 1 {
			return nil, nil
		}
		return []int{n}, nil
	})

	out, err := flow.Collect(ctx, evens(flow.Source(ctx, 1, 2, 3, 4)))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestFlatMapTypeChange(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	bytes := flow.FlatMap(ctx, func(s string) ([]int, error) {
		out := make([]int, len(s))
		for i := 0; i < len(s); i++ {
			out[i] = int(s[i])
		}
		return out, nil
	})

	out, err := flow.Collect(ctx, bytes(flow.Source(ctx, "ab", "c")))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{97, 98, 99}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestFlatMapError(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	boom := errors.New("boom")
	f := flow.FlatMap(ctx, func(n int) ([]int, error) {
		if n == 3 {
			return nil, boom
		}
		return []int{n}, nil
	})

	_, err := flow.Collect(ctx, f(flow.Source(ctx, 1, 2, 3, 4)))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

// The key ordering guarantee: FlatMap fanned out across workers, then merged,
// must reconstruct both input order AND sibling order deterministically.
func TestFlatMapFanOutInOrder(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	dup := flow.FlatMap(ctx, func(n int) ([]int, error) { return []int{n, -n}, nil })

	in := flow.Source(ctx, 1, 2, 3, 4)
	branches := flow.FanOutN(ctx, in, 2, flow.RoundRobin(), dup)
	merged := flow.FanIn(ctx, branches)

	out, err := flow.CollectOrdered(ctx, merged, flow.InOrder)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, -1, 2, -2, 3, -3, 4, -4}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

// Reverse must reverse siblings too, which is why Element carries a sub-position.
// Without it, this would wrongly yield [4,-4,3,-3,...].
func TestFlatMapReverse(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	dup := flow.FlatMap(ctx, func(n int) ([]int, error) { return []int{n, -n}, nil })

	in := flow.Source(ctx, 1, 2, 3, 4)
	branches := flow.FanOutN(ctx, in, 2, flow.RoundRobin(), dup)
	merged := flow.FanIn(ctx, branches)

	out, err := flow.CollectOrdered(ctx, merged, flow.Reverse)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{-4, 4, -3, 3, -2, 2, -1, 1}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}
