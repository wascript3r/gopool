package gopool

import (
	"sync"
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
