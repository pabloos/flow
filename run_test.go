package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pabloos/flow"
)

// One Processor interface does Map, Filter and FlatMap depending on how many
// times it emits.
func TestProcessorIsMapFilterFlatMap(t *testing.T) {
	proc := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		if n%2 == 1 {
			return nil // Filter: drop odds
		}
		if err := emit(n); err != nil { // Map: emit the value
			return err
		}
		return emit(n * 10) // FlatMap: and another
	})

	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3, 4), proc, flow.Into(&got))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 20, 4, 40}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Ordered reconstructs input order across a worker pool.
func TestOrderedFanOut(t *testing.T) {
	double := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * 2)
	})

	nums := make([]int, 0, 20)
	want := make([]int, 0, 20)
	for i := 1; i <= 20; i++ {
		nums = append(nums, i)
		want = append(want, i*2)
	}

	var got []int
	err := flow.Run(context.Background(), flow.Slice(nums...), double, flow.Into(&got),
		flow.Workers(4), flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Without Ordered, arrival order is nondeterministic but the multiset holds.
func TestUnorderedSameSet(t *testing.T) {
	ten := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * 10)
	})

	nums := make([]int, 0, 30)
	want := make([]int, 0, 30)
	for i := 1; i <= 30; i++ {
		nums = append(nums, i)
		want = append(want, i*10)
	}

	var got []int
	err := flow.Run(context.Background(), flow.Slice(nums...), ten, flow.Into(&got),
		flow.Workers(6))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Then composes two processors with a type change, in-process, no channel.
func TestThenTypeChange(t *testing.T) {
	length := flow.ProcessorFunc[string, int](func(ctx context.Context, s string, emit func(int) error) error {
		return emit(len(s))
	})
	label := flow.ProcessorFunc[int, string](func(ctx context.Context, n int, emit func(string) error) error {
		return emit(fmt.Sprintf("len=%d", n))
	})

	var got []string
	err := flow.Run(context.Background(), flow.Slice("a", "bb", "ccc"),
		flow.Then(length, label), flow.Into(&got),
		flow.Workers(3), flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"len=1", "len=2", "len=3"}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestProcessorErrorFailFast(t *testing.T) {
	boom := errors.New("boom")
	proc := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		if n == 3 {
			return boom
		}
		return emit(n)
	})

	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3, 4, 5), proc, flow.Into(&got),
		flow.Workers(2))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestConsumerErrorFailFast(t *testing.T) {
	boom := errors.New("sink failed")
	identity := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n)
	})
	sink := flow.Each(func(n int) error {
		if n == 3 {
			return boom
		}
		return nil
	})

	err := flow.Run(context.Background(), flow.Slice(1, 2, 3, 4, 5), identity, sink)
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestFromSeqProducer(t *testing.T) {
	square := flow.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * n)
	})

	var got []int
	err := flow.Run(context.Background(),
		flow.FromSeq(slices.Values([]int{2, 3, 4})), square, flow.Into(&got),
		flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{4, 9, 16}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// A cancelled caller context must come back as an error: otherwise Run returns
// nil after consuming only part of the input and the caller cannot tell a
// truncated run from a finished one.
func TestCancelledContextIsReported(t *testing.T) {
	double := flow.Map(func(n int) int { return n * 2 })

	t.Run("cancelled before the run starts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var out []int
		err := flow.Run(ctx, flow.Slice(1, 2, 3, 4, 5), double, flow.Into(&out))

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if len(out) != 0 {
			t.Fatalf("consumed %d values with a dead context, want 0", len(out))
		}
	})

	t.Run("cancelled midway", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var out []int
		err := flow.Run(ctx, flow.Slice(1, 2, 3, 4, 5, 6, 7, 8), double,
			flow.ConsumerFunc[int](func(_ context.Context, v int) error {
				out = append(out, v)
				if len(out) == 2 {
					cancel()
				}
				return nil
			}))

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if len(out) == 8 {
			t.Fatal("consumed the whole input after cancelling midway")
		}
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		slow := flow.Map(func(n int) int {
			time.Sleep(20 * time.Millisecond)
			return n
		})

		var out []int
		err := flow.Run(ctx, flow.Slice(1, 2, 3, 4, 5, 6, 7, 8), slow, flow.Into(&out))

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("a pipeline error wins over cancellation", func(t *testing.T) {
		boom := errors.New("boom")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var out []int
		err := flow.Run(ctx, flow.Slice(1, 2, 3, 4, 5),
			flow.TryMap(func(n int) (int, error) {
				if n == 3 {
					return 0, boom
				}
				return n, nil
			}),
			flow.Into(&out))

		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
	})

	t.Run("an uncancelled run still returns nil", func(t *testing.T) {
		var out []int
		if err := flow.Run(context.Background(), flow.Slice(1, 2, 3), double, flow.Into(&out)); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if len(out) != 3 {
			t.Fatalf("consumed %d values, want 3", len(out))
		}
	})
}
