package gopool

import (
	"runtime"
	"sync"
)

var maxProcs = runtime.GOMAXPROCS(0)

// Preset pool resizing strategies
var (
	// Eager maximizes responsiveness at the expense of higher resource usage,
	// which can reduce throughput under certain conditions.
	// This strategy is meant for worker pools that will operate at a small percentage of their capacity
	// most of the time and may occasionally receive bursts of tasks. It's the default strategy.
	Eager = func() ResizingStrategy { return NewRatedResizer(1) }
	// Balanced tries to find a balance between responsiveness and throughput.
	// It's suitable for general purpose worker pools or those
	// that will operate close to 50% of their capacity most of the time.
	Balanced = func() ResizingStrategy { return NewRatedResizer(maxProcs / 2) }
	// Lazy maximizes throughput at the expense of responsiveness.
	// This strategy is meant for worker pools that will operate close to their max. capacity most of the time.
	Lazy = func() ResizingStrategy { return NewRatedResizer(maxProcs) }
)

// ratedResizer implements a rated resizing strategy
type ratedResizer struct {
	mx   sync.Mutex
	rate int
	hits int
}

// NewRatedResizer creates a resizing strategy which can be configured
// to create workers at a specific rate when the pool has no idle workers.
// rate: determines the number of tasks to receive before creating an extra worker.
// A value of 3 can be interpreted as: "Create a new worker every 3 tasks".
func NewRatedResizer(rate int) *ratedResizer {
	if rate < 1 {
		rate = 1
	}

	return &ratedResizer{
		mx:   sync.Mutex{},
		rate: rate,
		hits: 0,
	}
}

func (r *ratedResizer) Resize(runningWorkers int) bool {
	if r.rate == 1 || runningWorkers == 0 {
		return true
	}

	r.mx.Lock()
	defer r.mx.Unlock()

	r.hits = (r.hits + 1) % r.rate
	return r.hits == 1
}
