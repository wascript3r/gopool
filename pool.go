// Package gopool contains tools for goroutine reuse.
package gopool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultIdleTimeout = 10 * time.Second
)

var (
	// ErrScheduleTimeout is returned when there are no free goroutines during some period of time.
	ErrScheduleTimeout = errors.New("schedule error: timed out")

	// ErrPoolTerminated is returned when the schedule method is called, but the Pool is already terminated.
	ErrPoolTerminated = errors.New("schedule error: pool is terminated")

	// ErrTaskIsNil is returned when the schedule method is called with nil task.
	ErrTaskIsNil = errors.New("schedule error: task is nil")
)

// ResizingStrategy represents a pool resizing strategy
type ResizingStrategy interface {
	Resize(runningWorkers int) bool
}

// Option represents an option that can be passed when instantiating a worker pool to customize it
type Option func(*Pool)

// IdleTimeout allows to change the idle timeout for a worker pool
func IdleTimeout(idleTimeout time.Duration) Option {
	return func(pool *Pool) {
		pool.idleTimeout = idleTimeout
	}
}

// MinWorkers allows to change the minimum number of workers of a worker pool
func MinWorkers(minWorkers int) Option {
	return func(pool *Pool) {
		pool.minWorkers = minWorkers
	}
}

// Strategy allows to change the strategy used to resize the pool
func Strategy(strategy ResizingStrategy) Option {
	return func(pool *Pool) {
		pool.strategy = strategy
	}
}

// Pool contains logic of goroutine reuse.
type Pool struct {
	maxWorkers  int
	minWorkers  int
	idleTimeout time.Duration

	runningWorkers int32
	idleWorkers    int32

	stopWorkersC chan struct{}
	stopTasksC   chan struct{}
	workC        chan func()

	workersWg sync.WaitGroup
	tasksWg   sync.WaitGroup

	strategy ResizingStrategy
	once     sync.Once
}

// New creates a new Pool that can be used to schedule tasks.
func New(maxWorkers, queueSize int, opts ...Option) *Pool {
	p := &Pool{
		maxWorkers:  maxWorkers,
		minWorkers:  0,
		idleTimeout: defaultIdleTimeout,

		runningWorkers: 0,
		idleWorkers:    0,

		stopWorkersC: make(chan struct{}),
		stopTasksC:   make(chan struct{}),
		workC:        make(chan func(), queueSize),

		workersWg: sync.WaitGroup{},
		tasksWg:   sync.WaitGroup{},

		strategy: Eager(),
		once:     sync.Once{},
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.maxWorkers <= 0 {
		panic("max workers must be greater than zero")
	}
	if p.minWorkers < 0 {
		panic("min workers must be greater or equal to zero")
	}
	if p.minWorkers > p.maxWorkers {
		panic("min workers must be less or equal to max workers")
	}
	if queueSize < 0 {
		panic("queue size must be greater or equal to zero")
	}

	p.workersWg.Add(1)
	go p.purge()

	for i := 0; i < p.minWorkers; i++ {
		p.spawn(nil)
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

// ErrGroup creates a new error group
func (p *Pool) ErrGroup(ctx context.Context) (*ErrGroup, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &ErrGroup{
		pool:    p,
		wg:      sync.WaitGroup{},
		cancel:  cancel,
		errOnce: sync.Once{},
		err:     nil,
	}, ctx
}

// MaxWorkers returns the pool size
func (p *Pool) MaxWorkers() int {
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

func (p *Pool) purge() {
	defer p.workersWg.Done()

	ticker := time.NewTicker(p.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopWorkersC:
			return

		case <-ticker.C:
			if p.IdleWorkers() > 0 && p.RunningWorkers() > p.minWorkers {
				p.workC <- nil
			}
		}
	}
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
	if (p.IdleWorkers() == 0 && runningWorkers < p.maxWorkers && p.strategy.Resize(runningWorkers)) || runningWorkers < p.minWorkers {
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
	defer func() {
		atomic.AddInt32(&p.idleWorkers, 1)
	}()

	if !firstTask {
		atomic.AddInt32(&p.idleWorkers, -1)
	}
	task()
}

func (p *Pool) worker(task func()) {
	if task != nil {
		p.executeTask(task, true)
	} else {
		atomic.AddInt32(&p.idleWorkers, 1)
	}

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
