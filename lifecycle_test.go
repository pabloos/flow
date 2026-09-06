package flow_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

func TestEmptySource(t *testing.T) {
	ctx, cancel := flow.New(context.Background())
	defer cancel()

	stage := flow.Map(ctx, func(n int) (int, error) { return n, nil })

	out, err := flow.Collect(ctx, stage(flow.Source[int](ctx)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("want empty, got %v", out)
	}
}

// Cancelling mid-consumption must unwind every stage goroutine; a leak would
// leave goroutines parked on channel sends.
func TestCancellationUnwinds(t *testing.T) {
	base := runtime.NumGoroutine()

	ctx, cancel := flow.New(context.Background())

	nums := make([]int, 1000)
	stage := flow.Map(ctx, func(n int) (int, error) { return n, nil })
	out := stage(flow.Source(ctx, nums...))

	consumed := 0
	for range flow.Seq(out) {
		consumed++
		if consumed == 5 {
			cancel()
			break
		}
	}

	eventuallyGoroutines(t, base+2)
}

// eventuallyGoroutines waits until the live goroutine count settles at or below
// max, tolerating the small delay between cancellation and goroutine exit.
func eventuallyGoroutines(t *testing.T, max int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		if n := runtime.NumGoroutine(); n <= max {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not settle: have %d, want <= %d", runtime.NumGoroutine(), max)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
