// Package gopool contains tools for goroutine reuse.
package gopool

import (
	"errors"
	"sync"
	"time"
)

// ErrScheduleTimeout is returned when there are no free
// goroutines during some period of time.
var ErrScheduleTimeout = errors.New("schedule error: timed out")

// ErrPoolTerminated is returned when the schedule method is called, but the Pool is already terminated.
var ErrPoolTerminated = errors.New("schedule error: pool is terminated")

// Pool contains logic of goroutine reuse.
type Pool struct {
	size int

	endC   chan struct{}
	workC  chan func()
	spawnC chan struct{}

	workersWg sync.WaitGroup
	tasksWg   sync.WaitGroup

	once sync.Once
}

// New creates new goroutine pool with given size. It also creates a work
// queue of given size. Finally, it spawns given amount of goroutines
// immediately.
func New(size, queue, spawn int) *Pool {
	if size <= 0 {
		panic("size must be greater than zero")
	}
	if queue < 0 {
		panic("queue must be greater than or equal to zero")
	}
	if spawn < 0 {
		panic("spawn must be greater than or equal to zero")
	}

	if spawn == 0 && queue > 0 {
		panic("dead queue configuration")
	}
	if spawn > size {
		panic("spawn must be less than or equal to size")
	}

	p := &Pool{
		size: size,

		endC:   make(chan struct{}),
		workC:  make(chan func(), queue),
		spawnC: make(chan struct{}, size),

		workersWg: sync.WaitGroup{},
		tasksWg:   sync.WaitGroup{},

		once: sync.Once{},
	}

	for i := 0; i < spawn; i++ {
		p.spawnC <- struct{}{}
		p.spawn(nil)
	}

	return p
}

// GetSize returns the pool size
func (p *Pool) GetSize() int {
	return p.size
}

// Schedule schedules task to be executed over pool's workers with no provided timeout.
func (p *Pool) Schedule(task func()) error {
	return p.schedule(nil, task)
}

// ScheduleTimeout schedules task to be executed over pool's workers with provided timeout.
func (p *Pool) ScheduleTimeout(timeout time.Duration, task func()) error {
	return p.schedule(time.After(timeout), task)
}

func (p *Pool) schedule(timeout <-chan time.Time, task func()) error {
	p.tasksWg.Add(1)
	defer p.tasksWg.Done()

	select {
	case <-p.endC:
		return ErrPoolTerminated

	default:
	}

	select {
	case <-p.endC:
		return ErrPoolTerminated

	case p.workC <- task:
		return nil

	case p.spawnC <- struct{}{}:
		p.spawn(task)
		return nil

	case <-timeout:
		return ErrScheduleTimeout
	}
}

func (p *Pool) spawn(task func()) {
	p.workersWg.Add(1)
	go p.worker(task)
}

func (p *Pool) worker(task func()) {
	defer func() {
		<-p.spawnC
		p.workersWg.Done()
	}()

	for {
		if task != nil {
			task()
		}

		select {
		case task = <-p.workC:
			continue

		case <-p.endC:
			return
		}
	}
}

// End returns the read-only channel which indicates when the Pool is terminated.
func (p *Pool) End() <-chan struct{} {
	return p.endC
}

// StopAndWait blocks until all workers are terminated and closes all channels.
func (p *Pool) StopAndWait() {
	p.once.Do(func() {
		close(p.endC)

		p.tasksWg.Wait()
		p.workersWg.Wait()
		close(p.workC)
		close(p.spawnC)
	})
}
