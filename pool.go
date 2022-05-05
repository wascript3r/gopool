// Package gopool contains tools for goroutine reuse.
package gopool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrScheduleTimeout is returned when there are no free goroutines during some period of time.
	ErrScheduleTimeout = errors.New("schedule error: timed out")

	// ErrPoolTerminated is returned when the schedule method is called, but the Pool is already terminated.
	ErrPoolTerminated = errors.New("schedule error: pool is terminated")

	// ErrTaskIsNil is returned when the schedule method is called with nil task.
	ErrTaskIsNil = errors.New("schedule error: task is nil")
)

// Pool contains logic of goroutine reuse.
type Pool struct {
	maxWorkers     int
	runningWorkers int32
	idleWorkers    int32

	stopWorkersC chan struct{}
	stopTasksC   chan struct{}
	workC        chan func()

	workersWg sync.WaitGroup
	tasksWg   sync.WaitGroup

	resizer *ratedResizer
	once    sync.Once
}

// New creates new goroutine pool with given size. It also creates a work
// queue of given size. Finally, it spawns given amount of goroutines
// immediately.
func New(maxWorkers, queueSize int) *Pool {
	if maxWorkers <= 0 {
		panic("maxWorkers must be greater than zero")
	}
	if queueSize < 0 {
		panic("queueSize must be greater than or equal to zero")
	}

	p := &Pool{
		maxWorkers:     maxWorkers,
		runningWorkers: 0,
		idleWorkers:    0,

		stopWorkersC: make(chan struct{}),
		stopTasksC:   make(chan struct{}),
		workC:        make(chan func(), queueSize),

		workersWg: sync.WaitGroup{},
		tasksWg:   sync.WaitGroup{},

		resizer: NewRatedResizer(4),
		once:    sync.Once{},
	}

	return p
}

// TaskGroup creates a new task group
func (p *Pool) TaskGroup() *TaskGroup {
	return &TaskGroup{
		pool: p,
		wg:   sync.WaitGroup{},
	}
}

// GetSize returns the pool size
func (p *Pool) GetSize() int {
	return p.maxWorkers
}

// RunningWorkers returns the number of running workers
func (p *Pool) RunningWorkers() int {
	return int(atomic.LoadInt32(&p.runningWorkers))
}

// IdleWorkers returns the number of idle workers who are waiting for tasks
func (p *Pool) IdleWorkers() int {
	return int(atomic.LoadInt32(&p.idleWorkers))
}

// Schedule schedules task to be executed over pool's workers with no provided timeout.
func (p *Pool) Schedule(task func()) error {
	return p.schedule(task, nil)
}

// ScheduleTimeout schedules task to be executed over pool's workers with provided timeout.
func (p *Pool) ScheduleTimeout(task func(), timeout time.Duration) error {
	return p.schedule(task, time.After(timeout))
}

func (p *Pool) schedule(task func(), timeout <-chan time.Time) (err error) {
	if task == nil {
		panic(ErrTaskIsNil)
	}
	p.tasksWg.Add(1)
	defer func() {
		if err != nil {
			p.tasksWg.Done()
		}
	}()

	select {
	case <-p.stopTasksC:
		return ErrPoolTerminated

	default:
	}

	runningWorkers := p.RunningWorkers()
	if p.IdleWorkers() == 0 && runningWorkers < p.maxWorkers && p.resizer.Resize(runningWorkers) {
		p.spawn(task)
		return nil
	}

	select {
	case <-p.stopTasksC:
		return ErrPoolTerminated

	case p.workC <- task:
		return nil

	case <-timeout:
		return ErrScheduleTimeout
	}
}

func (p *Pool) spawn(task func()) {
	p.workersWg.Add(1)
	atomic.AddInt32(&p.runningWorkers, 1)
	go p.worker(task)
}

func (p *Pool) executeTask(task func(), firstTask bool) {
	defer p.tasksWg.Done()

	if !firstTask {
		atomic.AddInt32(&p.idleWorkers, -1)
	}
	task()
	atomic.AddInt32(&p.idleWorkers, 1)
}

func (p *Pool) worker(task func()) {
	p.executeTask(task, true)

	defer func() {
		atomic.AddInt32(&p.idleWorkers, -1)
		atomic.AddInt32(&p.runningWorkers, -1)
		p.workersWg.Done()
	}()

	for {
		select {
		case task = <-p.workC:
			if task == nil {
				return
			}
			p.executeTask(task, false)

		case <-p.stopWorkersC:
			return
		}
	}
}

// Stopped returns the read-only channel which indicates when the Pool is stopped.
func (p *Pool) Stopped() <-chan struct{} {
	return p.stopTasksC
}

// StopAndWait blocks until all workers are terminated and closes all channels.
func (p *Pool) StopAndWait() {
	p.once.Do(func() {
		close(p.stopTasksC)
		p.tasksWg.Wait()

		close(p.stopWorkersC)
		p.workersWg.Wait()
		close(p.workC)
	})
}
