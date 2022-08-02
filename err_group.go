package gopool

import (
	"sync"
	"time"
)

type ErrGroup struct {
	pool *Pool

	wg      sync.WaitGroup
	cancel  func()
	errOnce sync.Once
	err     error
}

func (g *ErrGroup) stop(err error) {
	g.errOnce.Do(func() {
		g.err = err
		if g.cancel != nil {
			g.cancel()
		}
	})
}

// Schedule adds a task to this group and sends it to the worker pool to be executed
func (g *ErrGroup) Schedule(task func() error) {
	g.wg.Add(1)

	err := g.pool.Schedule(func() {
		defer g.wg.Done()

		if err := task(); err != nil {
			g.stop(err)
		}
	})
	if err != nil {
		g.stop(err)
		g.wg.Done()
	}
}

// ScheduleTimeout adds a task to this group and sends it to the worker pool to be executed with a timeout
func (g *ErrGroup) ScheduleTimeout(task func() error, timeout time.Duration) {
	g.wg.Add(1)

	err := g.pool.ScheduleTimeout(func() {
		defer g.wg.Done()

		if err := task(); err != nil {
			g.stop(err)
		}
	}, timeout)
	if err != nil {
		g.stop(err)
		g.wg.Done()
	}
}

// Wait waits until all the tasks in this group have completed, then
// returns the first non-nil error (if any) from them.
func (g *ErrGroup) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
}
