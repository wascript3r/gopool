package gopool

import (
	"sync"
	"time"
)

// TaskGroup represents a group of related tasks
type TaskGroup struct {
	pool *Pool
	wg   sync.WaitGroup
}

// Schedule adds a task to this group and sends it to the worker pool to be executed
func (g *TaskGroup) Schedule(task func()) error {
	g.wg.Add(1)

	err := g.pool.Schedule(func() {
		defer g.wg.Done()
		task()
	})
	if err != nil {
		g.wg.Done()
		return err
	}

	return nil
}

// ScheduleTimeout adds a task to this group and sends it to the worker pool to be executed with a timeout
func (g *TaskGroup) ScheduleTimeout(task func(), timeout time.Duration) error {
	g.wg.Add(1)

	err := g.pool.ScheduleTimeout(func() {
		defer g.wg.Done()
		task()
	}, timeout)
	if err != nil {
		g.wg.Done()
		return err
	}

	return nil
}

// Wait waits until all the tasks in this group have completed
func (g *TaskGroup) Wait() {
	g.wg.Wait()
}
