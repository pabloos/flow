package pipe_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/pabloos/flow/x/pipe"
)

func TestMapConstructor(t *testing.T) {
	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3),
		pipe.Map(func(n int) int { return n * 2 }), pipe.Into(&got),
		pipe.Workers(3), pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFilterConstructor(t *testing.T) {
	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3, 4, 5, 6),
		pipe.Filter(func(n int) bool { return n%2 == 0 }), pipe.Into(&got),
		pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFlatMapConstructor(t *testing.T) {
	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3),
		pipe.FlatMap(func(n int) []int { return []int{n, -n} }), pipe.Into(&got),
		pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, -1, 2, -2, 3, -3}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

// The terse constructors compose with Then, mixing type-changing stages.
func TestConstructorsComposeWithThen(t *testing.T) {
	length := pipe.Map(func(s string) int { return len(s) })
	big := pipe.Filter(func(n int) bool { return n > 1 })

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice("a", "bb", "ccc"),
		pipe.Then(length, big), pipe.Into(&got),
		pipe.Workers(2), pipe.Ordered())
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 3}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestTryMapError(t *testing.T) {
	boom := errors.New("boom")
	proc := pipe.TryMap(func(n int) (int, error) {
		if n == 3 {
			return 0, boom
		}
		return n, nil
	})

	var got []int
	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3, 4), proc, pipe.Into(&got),
		pipe.Workers(2))
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestTryFilterAndTryFlatMapError(t *testing.T) {
	boom := errors.New("bad")

	err := pipe.Run(context.Background(), pipe.Slice(1, 2, 3),
		pipe.TryFilter(func(n int) (bool, error) {
			if n == 2 {
				return false, boom
			}
			return true, nil
		}), pipe.Into(new([]int)))
	if !errors.Is(err, boom) {
		t.Fatalf("TryFilter: want boom, got %v", err)
	}

	err = pipe.Run(context.Background(), pipe.Slice(1, 2, 3),
		pipe.TryFlatMap(func(n int) ([]int, error) {
			if n == 2 {
				return nil, boom
			}
			return []int{n}, nil
		}), pipe.Into(new([]int)))
	if !errors.Is(err, boom) {
		t.Fatalf("TryFlatMap: want boom, got %v", err)
	}
}
