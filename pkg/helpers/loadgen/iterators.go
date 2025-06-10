// loadgen/iterators.go
package loadgen

import "sync/atomic"

// Sequence provides a thread-safe iterator for generating sequential integers,
// useful for message counters in payloads.
type Sequence struct {
	val int64
}

// NewSequence creates a new sequence starting at the given value.
// The first call to Next() will return startValue.
func NewSequence(startValue int64) *Sequence {
	// Initialize to startValue - 1 so the first atomic add results in startValue.
	return &Sequence{val: startValue - 1}
}

// Next atomically increments the sequence and returns the new value.
// This is safe to call from multiple goroutines.
func (s *Sequence) Next() int64 {
	return atomic.AddInt64(&s.val, 1)
}
