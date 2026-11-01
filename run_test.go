package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
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

// Prefetch is the in-flight window, and it must mean the same thing in both
// entry points: n for Run, which queues in one shared channel, and n per worker
// for RunPartitioned, where each worker owns its queue. It used to be applied to
// the shared intake *and* to every worker queue, holding n*(Workers+1) items.
func TestPrefetchWindow(t *testing.T) {
	const prefetch, workers = 10, 4

	measure := func(partitioned bool) int64 {
		var emitted atomic.Int64
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		prod := flow.ProducerFunc[int](func(ctx context.Context, emit func(int) error) error {
			for i := 0; ; i++ {
				if err := emit(i); err != nil {
					return err
				}
				emitted.Add(1)
			}
		})
		proc := flow.Map(func(n int) int { return n })
		held := make(chan struct{})
		cons := flow.ConsumerFunc[int](func(ctx context.Context, v int) error {
			<-held // the sink never drains, so the pipeline fills and stops
			return nil
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			if partitioned {
				_ = flow.RunPartitioned(ctx, prod, proc, cons,
					func(n int) int { return n }, // spread over every worker
					flow.Workers(workers), flow.Prefetch(prefetch))
			} else {
				_ = flow.Run(ctx, prod, proc, cons,
					flow.Workers(workers), flow.Prefetch(prefetch))
			}
		}()

		time.Sleep(300 * time.Millisecond) // let it saturate
		n := emitted.Load()
		cancel()
		close(held)
		<-done
		return n
	}

	// Beyond the queues themselves, each worker can hold one item in Process and
	// one waiting to deliver, plus one in the collector: a small, bounded slack.
	slack := int64(2*workers + 2)

	shared := measure(false)
	if max := int64(prefetch) + slack; shared > max {
		t.Errorf("Run in-flight = %d, want <= %d", shared, max)
	}
	perWorker := measure(true)
	if max := int64(prefetch*workers) + slack; perWorker > max {
		t.Errorf("RunPartitioned in-flight = %d, want <= %d", perWorker, max)
	}
	t.Logf("Prefetch(%d), Workers(%d): Run=%d  RunPartitioned=%d",
		prefetch, workers, shared, perWorker)
}

// StageError names the end that failed and keeps the original error reachable.
func TestStageErrorIdentifiesFailingEnd(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name string
		want flow.Stage
		run  func() error
	}{
		{"producer", flow.StageProducer, func() error {
			return flow.Run(context.Background(),
				flow.ProducerFunc[int](func(ctx context.Context, emit func(int) error) error {
					return boom
				}),
				flow.Map(func(n int) int { return n }), flow.Into(new([]int)))
		}},
		{"processor", flow.StageProcessor, func() error {
			return flow.Run(context.Background(), flow.Slice(1, 2, 3),
				flow.TryMap(func(n int) (int, error) { return 0, boom }),
				flow.Into(new([]int)))
		}},
		{"consumer", flow.StageConsumer, func() error {
			return flow.Run(context.Background(), flow.Slice(1, 2, 3),
				flow.Map(func(n int) int { return n }),
				flow.Each(func(n int) error { return boom }))
		}},
	}

	for _, tc := range cases {
		err := tc.run()
		var se *flow.StageError
		if !errors.As(err, &se) {
			t.Errorf("%s: err = %v, want a *flow.StageError", tc.name, err)
			continue
		}
		if se.Stage != tc.want {
			t.Errorf("%s: stage = %s, want %s", tc.name, se.Stage, tc.want)
		}
		if !errors.Is(err, boom) {
			t.Errorf("%s: original error unreachable through %v", tc.name, err)
		}
	}
}

// A run stopped by its caller returns the context error unwrapped: no stage
// failed, so there is nothing to name.
func TestCallerCancellationIsNotAStageError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := flow.Run(ctx, flow.Slice(1, 2, 3),
		flow.Map(func(n int) int { return n }), flow.Into(new([]int)))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	var se *flow.StageError
	if errors.As(err, &se) {
		t.Errorf("err = %v, want it not wrapped as a StageError", err)
	}
}
