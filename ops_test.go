package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pabloos/flow"
)

func TestMapConstructor(t *testing.T) {
	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3),
		flow.Map(func(n int) int { return n * 2 }), flow.Into(&got),
		flow.Workers(3), flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFilterConstructor(t *testing.T) {
	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3, 4, 5, 6),
		flow.Filter(func(n int) bool { return n%2 == 0 }), flow.Into(&got),
		flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFlatMapConstructor(t *testing.T) {
	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3),
		flow.FlatMap(func(n int) []int { return []int{n, -n} }), flow.Into(&got),
		flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, -1, 2, -2, 3, -3}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// The terse constructors compose with Then, mixing type-changing stages.
func TestConstructorsComposeWithThen(t *testing.T) {
	length := flow.Map(func(s string) int { return len(s) })
	big := flow.Filter(func(n int) bool { return n > 1 })

	var got []int
	err := flow.Run(context.Background(), flow.Slice("a", "bb", "ccc"),
		flow.Then(length, big), flow.Into(&got),
		flow.Workers(2), flow.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 3}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestTryMapError(t *testing.T) {
	boom := errors.New("boom")
	proc := flow.TryMap(func(n int) (int, error) {
		if n == 3 {
			return 0, boom
		}
		return n, nil
	})

	var got []int
	err := flow.Run(context.Background(), flow.Slice(1, 2, 3, 4), proc, flow.Into(&got),
		flow.Workers(2))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestTryFilterAndTryFlatMapError(t *testing.T) {
	boom := errors.New("bad")

	err := flow.Run(context.Background(), flow.Slice(1, 2, 3),
		flow.TryFilter(func(n int) (bool, error) {
			if n == 2 {
				return false, boom
			}
			return true, nil
		}), flow.Into(new([]int)))
	if !errors.Is(err, boom) {
		t.Fatalf("TryFilter: want boom, got %v", err)
	}

	err = flow.Run(context.Background(), flow.Slice(1, 2, 3),
		flow.TryFlatMap(func(n int) ([]int, error) {
			if n == 2 {
				return nil, boom
			}
			return []int{n}, nil
		}), flow.Into(new([]int)))
	if !errors.Is(err, boom) {
		t.Fatalf("TryFlatMap: want boom, got %v", err)
	}
}
