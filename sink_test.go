package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pabloos/flow"
)

func TestForEach(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	// ForEach consumes in the calling goroutine, so no synchronization needed.
	sum := 0
	err := flow.ForEach(ctx, flow.Source(ctx, 1, 2, 3, 4), func(n int) error {
		sum += n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum != 10 {
		t.Fatalf("sum = %d, want 10", sum)
	}
}

func TestForEachError(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	boom := errors.New("stop")
	err := flow.ForEach(ctx, flow.Source(ctx, 1, 2, 3, 4, 5), func(n int) error {
		if n == 3 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestReduce(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	sum, err := flow.Reduce(ctx, flow.Source(ctx, 1, 2, 3, 4, 5), 0,
		func(acc, n int) int { return acc + n })
	if err != nil {
		t.Fatal(err)
	}
	if sum != 15 {
		t.Fatalf("sum = %d, want 15", sum)
	}
}
