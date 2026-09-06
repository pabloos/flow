package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/pabloos/flow"
)

func TestFilterError(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	boom := errors.New("bad predicate")
	odd := flow.Filter(ctx, func(n int) (bool, error) {
		if n == 3 {
			return false, boom
		}
		return n%2 == 1, nil
	})

	_, err := flow.Collect(ctx, odd(flow.Source(ctx, 1, 2, 3, 4)))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestWithBufferPreservesResults(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	// Buffering changes scheduling, not results or order.
	stage := flow.Map(ctx, func(n int) (int, error) { return n + 1, nil }, flow.WithBuffer(4))

	out, err := flow.Collect(ctx, stage(flow.Source(ctx, 1, 2, 3, 4, 5)))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 3, 4, 5, 6}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}

// Nested Then builds a type-changing pipeline string -> int -> float64 -> string.
func TestNestedThen(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	a := flow.Map(ctx, func(s string) (int, error) { return len(s), nil })
	b := flow.Map(ctx, func(n int) (float64, error) { return float64(n), nil })
	c := flow.Map(ctx, func(f float64) (string, error) { return fmt.Sprintf("%.0f", f*2), nil })

	pipe := flow.Then(flow.Then(a, b), c) // Stage[string, string]

	out, err := flow.Collect(ctx, pipe(flow.Source(ctx, "x", "yy", "zzz")))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"2", "4", "6"}; !slices.Equal(out, want) {
		t.Fatalf("got %v want %v", out, want)
	}
}
