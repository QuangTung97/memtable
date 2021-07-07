package memtable

type memtableOptions struct {
	numBuckets    uint32
	entryListSize uint32
	leaseTimeout  uint32
}

// Option ...
type Option func(opts *memtableOptions)

func computeOptions(options ...Option) memtableOptions {
	result := memtableOptions{
		numBuckets:    1024,
		entryListSize: 16,
		leaseTimeout:  30,
	}

	for _, o := range options {
		o(&result)
	}
	return result
}

// WithNumBuckets configures number of lease buckets
func WithNumBuckets(n uint32) Option {
	return func(opts *memtableOptions) {
		opts.numBuckets = ceilPowerOfTwo(n)
	}
}

// WithLeaseListSize configures the number of entries in a lease list
func WithLeaseListSize(n uint32) Option {
	return func(opts *memtableOptions) {
		opts.entryListSize = ceilPowerOfTwo(n)
	}
}

// WithLeaseTimeout for duration of lease timeout, in second
func WithLeaseTimeout(d uint32) Option {
	return func(opts *memtableOptions) {
		opts.leaseTimeout = d
	}
}
