package flow

import (
	"context"
	"sync"
)

// FanIn merges several streams into one. Elements arrive interleaved and
// nondeterministically ordered, but each keeps its origin order so that
// CollectOrdered can restore the input sequence. WithBuffer sizes the merged
// output channel.
func FanIn[T any](ctx context.Context, ins []Stream[T], opts ...Option) Stream[T] {
	cfg := newConfig(opts)
	out := make(Stream[T], cfg.buffer)

	var wg sync.WaitGroup
	wg.Add(len(ins))
	for _, in := range ins {
		go func(in Stream[T]) {
			defer wg.Done()
			for el := range in {
				select {
				case out <- el:
				case <-ctx.Done():
					return
				}
			}
		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
