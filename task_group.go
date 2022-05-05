package gopool

import "sync"

// TaskGroup represents a group of related tasks
type TaskGroup struct {
	pool *Pool
	wg   sync.WaitGroup
}

// Schedule adds a task to this group and sends it to the worker pool to be executed
func (g *TaskGroup) Schedule(task func()) error {
	g.wg.Add(1)
	return g.pool.Schedule(func() {
		defer g.wg.Done()
		task()
	})
}

// Wait waits until all the tasks in this group have completed
func (g *TaskGroup) Wait() {
	g.wg.Wait()
}
