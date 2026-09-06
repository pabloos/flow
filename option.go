package flow

type config struct {
	buffer int
}

// Option configures a stage or source (e.g. channel buffering).
type Option func(*config)

// WithBuffer sets the buffer size of the channel produced by a stage or source.
// The default is 0 (unbuffered: strict backpressure).
func WithBuffer(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.buffer = n
		}
	}
}

func newConfig(opts []Option) config {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	return c
}
