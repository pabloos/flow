package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pabloos/flow"
)

func TestMapFilterCollect(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	src := flow.Source(ctx, 1, 2, 3, 4, 5)
	even := flow.Filter(ctx, func(n int) (bool, error) { return n%2 == 0, nil })
	doubled := flow.Map(ctx, func(n int) (int, error) { return n * 2, nil })

	out, err := flow.Collect(ctx, doubled(even(src)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []int{4, 8}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestTypeChangingThen(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	parse := flow.Map(ctx, func(s string) (int, error) { return len(s), nil })
	scale := flow.Map(ctx, func(n int) (float64, error) { return float64(n) * 1.5, nil })
	stage := flow.Then(parse, scale) // Stage[string, float64]

	out, err := flow.Collect(ctx, stage(flow.Source(ctx, "a", "bb", "ccc")))
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{1.5, 3.0, 4.5}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestChainSameType(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	inc := flow.Map(ctx, func(n int) (int, error) { return n + 1, nil })
	dbl := flow.Map(ctx, func(n int) (int, error) { return n * 2, nil })
	pipe := flow.Chain(inc, dbl) // (n+1)*2

	out, err := flow.Collect(ctx, pipe(flow.Source(ctx, 1, 2, 3)))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{4, 6, 8}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

func TestFailFast(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	boom := errors.New("boom")
	stage := flow.Map(ctx, func(n int) (int, error) {
		if n == 3 {
			return 0, boom
		}
		return n, nil
	})

	_, err := flow.Collect(ctx, stage(flow.Source(ctx, 1, 2, 3, 4, 5)))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestIterBridges(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	seq := slices.Values([]int{10, 20, 30})
	doubled := flow.Map(ctx, func(n int) (int, error) { return n * 2, nil })

	var out []int
	for v := range flow.Seq(doubled(flow.From(ctx, seq))) {
		out = append(out, v)
	}
	if want := []int{20, 40, 60}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}
