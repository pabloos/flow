package pipe_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/pabloos/flow/x/pipe"
)

// One Processor interface does Map, Filter and FlatMap depending on how many
// times it emits.
func TestProcessorIsMapFilterFlatMap(t *testing.T) {
	proc := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		if n%2 == 1 {
			return nil // Filter: drop odds
		}
		if err := emit(n); err != nil { // Map: emit the value
			return err
		}
		return emit(n * 10) // FlatMap: and another
	})

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3, 4), proc, pipe.Into(&got))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 20, 4, 40}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Ordered reconstructs input order across a worker pool.
func TestOrderedFanOut(t *testing.T) {
	double := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * 2)
	})

	nums := make([]int, 0, 20)
	want := make([]int, 0, 20)
	for i := 1; i <= 20; i++ {
		nums = append(nums, i)
		want = append(want, i*2)
	}

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(nums...), double, pipe.Into(&got),
		pipe.Workers(4), pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// Without Ordered, arrival order is nondeterministic but the multiset holds.
func TestUnorderedSameSet(t *testing.T) {
	ten := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * 10)
	})

	nums := make([]int, 0, 30)
	want := make([]int, 0, 30)
	for i := 1; i <= 30; i++ {
		nums = append(nums, i)
		want = append(want, i*10)
	}

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(nums...), ten, pipe.Into(&got),
		pipe.Workers(6))
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
	length := pipe.ProcessorFunc[string, int](func(ctx context.Context, s string, emit func(int) error) error {
		return emit(len(s))
	})
	label := pipe.ProcessorFunc[int, string](func(ctx context.Context, n int, emit func(string) error) error {
		return emit(fmt.Sprintf("len=%d", n))
	})

	var got []string
	err := pipe.Run(context.Background(), pipe.Slice("a", "bb", "ccc"),
		pipe.Then(length, label), pipe.Into(&got),
		pipe.Workers(3), pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"len=1", "len=2", "len=3"}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestProcessorErrorFailFast(t *testing.T) {
	boom := errors.New("boom")
	proc := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		if n == 3 {
			return boom
		}
		return emit(n)
	})

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3, 4, 5), proc, pipe.Into(&got),
		pipe.Workers(2))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestConsumerErrorFailFast(t *testing.T) {
	boom := errors.New("sink failed")
	identity := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n)
	})
	sink := pipe.Each(func(n int) error {
		if n == 3 {
			return boom
		}
		return nil
	})

	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3, 4, 5), identity, sink)
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestFromSeqProducer(t *testing.T) {
	square := pipe.ProcessorFunc[int, int](func(ctx context.Context, n int, emit func(int) error) error {
		return emit(n * n)
	})

	var got []int
	err := pipe.Run(context.Background(),
		pipe.FromSeq(slices.Values([]int{2, 3, 4})), square, pipe.Into(&got),
		pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{4, 9, 16}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
