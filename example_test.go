package flow_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pabloos/flow"
)

// The basic shape: a producer, a processor, a consumer, run in parallel with
// the output order preserved.
func ExampleRun() {
	err := flow.Run(context.Background(),
		flow.Slice(1, 2, 3, 4),
		flow.Map(func(n int) int { return n * 2 }),
		flow.Each(func(n int) error {
			fmt.Println(n)
			return nil
		}),
		flow.Workers(4),
		flow.Ordered(),
	)
	if err != nil {
		fmt.Println("error:", err)
	}
	// Output:
	// 2
	// 4
	// 6
	// 8
}

// A realistic multi-stage pipeline composed with Then: parse, filter, double.
func ExampleThen() {
	pipeline := flow.Then(
		flow.TryMap(strconv.Atoi), // string -> int
		flow.Then(
			flow.Filter(func(n int) bool { return n > 0 }), // keep positives
			flow.Map(func(n int) int { return n * 2 }),     // double
		),
	)

	var out []int
	err := flow.Run(context.Background(),
		flow.Slice("1", "-2", "3", "4"),
		pipeline,
		flow.Into(&out),
		flow.Workers(4),
		flow.Ordered(),
	)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(out)
	// Output:
	// [2 6 8]
}

// One Processor can expand each input into several outputs (FlatMap) by
// emitting more than once.
func ExampleProcessorFunc() {
	split := flow.ProcessorFunc[string, string](func(ctx context.Context, line string, emit func(string) error) error {
		for _, w := range strings.Fields(line) {
			if err := emit(w); err != nil {
				return err
			}
		}
		return nil
	})

	var words []string
	err := flow.Run(context.Background(),
		flow.Slice("hello world", "goodbye"),
		split,
		flow.Into(&words),
		flow.Ordered(),
	)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(words)
	// Output:
	// [hello world goodbye]
}

// RunPartitioned keeps each key's values on one worker, in order. With Ordered
// the global input order is restored too.
func ExampleRunPartitioned() {
	type event struct {
		user string
		n    int
	}
	events := []event{{"a", 1}, {"b", 1}, {"a", 2}, {"b", 2}}

	err := flow.RunPartitioned(context.Background(),
		flow.Slice(events...),
		flow.Map(func(e event) string { return fmt.Sprintf("%s#%d", e.user, e.n) }),
		flow.Each(func(s string) error {
			fmt.Println(s)
			return nil
		}),
		func(e event) string { return e.user },
		flow.Workers(4),
		flow.Ordered(),
	)
	if err != nil {
		fmt.Println("error:", err)
	}
	// Output:
	// a#1
	// b#1
	// a#2
	// b#2
}

// sum is a stateful Consumer implemented on a struct rather than a closure.
type sum struct{ total int }

func (s *sum) Consume(ctx context.Context, n int) error {
	s.total += n
	return nil
}

// Ends can be structs when they carry state; the consumer is called from a
// single goroutine, so no locking is needed.
func Example_structConsumer() {
	total := &sum{}
	err := flow.Run(context.Background(),
		flow.Slice(1, 2, 3, 4),
		flow.Map(func(n int) int { return n }),
		total,
		flow.Workers(4),
	)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(total.total)
	// Output:
	// 10
}
