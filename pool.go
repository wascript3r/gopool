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
	end  chan struct{}

	mx  *sync.Mutex
	do  chan func()
	sem chan struct{}
	wg  *sync.WaitGroup

	once *sync.Once
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
		end:  make(chan struct{}),

		mx:  &sync.Mutex{},
		do:  make(chan func(), queue),
		sem: make(chan struct{}, size),
		wg:  &sync.WaitGroup{},

		once: &sync.Once{},
	}

	for i := 0; i < spawn; i++ {
		p.sem <- struct{}{}
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
	select {
	case <-p.end:
		return ErrPoolTerminated

	default:
	}

	select {
	case <-p.end:
		return ErrPoolTerminated

	case p.do <- task:
		return nil

	case p.sem <- struct{}{}:
		return p.spawn(task)

	case <-timeout:
		return ErrScheduleTimeout
	}
}

func (p *Pool) spawn(task func()) error {
	select {
	case <-p.end:
		return ErrPoolTerminated

	default:
		p.mx.Lock()
		p.wg.Add(1)
		p.mx.Unlock()

		go p.worker(task)
		return nil
	}
}

func (p *Pool) worker(task func()) {
	defer func() {
		<-p.sem
		p.wg.Done()
	}()

	for {
		if task != nil {
			task()
		}

		select {
		case task = <-p.do:
			continue

		case <-p.end:
			return
		}
	}
}

// End returns the read-only channel which indicates when the Pool is terminated.
func (p *Pool) End() <-chan struct{} {
	return p.end
}

// Terminate blocks until all workers are terminated and closes all channels.
func (p *Pool) Terminate() {
	p.once.Do(func() {
		close(p.end)

		p.mx.Lock()
		p.wg.Wait()
		close(p.do)
		close(p.sem)
		p.mx.Unlock()
	})
}
