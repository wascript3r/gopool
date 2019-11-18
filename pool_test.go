package gopool

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type tInt int

func (t tInt) Shedule(pool *Pool) {
	pool.Schedule(func() {
		t := time.NewTimer(time.Duration(t+1) * time.Second)
		select {
		case <-pool.End():
			t.Stop()
		case <-t.C:
		}
		// select {
		// case <-time.After(time.Duration(t+1) * time.Second):
		// case <-pool.End():
		// 	// 	time
		// }
		fmt.Println("exiting")
	})
}

type assertGoroutine struct {
	start int
}

func newAssertGoroutine() *assertGoroutine {
	runtime.GC()
	return &assertGoroutine{
		start: runtime.NumGoroutine(),
	}
}

func (a *assertGoroutine) equal(t *testing.T, delta int) {
	assert.Equal(t, a.start+delta, runtime.NumGoroutine())
}

func (a *assertGoroutine) end(t *testing.T) {
	runtime.GC()
	assert.Equal(t, a.start, runtime.NumGoroutine())
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func runScheduler(t *testing.T, poolSize, loopLen int) {
	ag := newAssertGoroutine()

	pool := New(poolSize, 0, 0)
	for i := 0; i < loopLen; i++ {
		tInt(i).Shedule(pool)
	}

	num := poolSize
	if poolSize > loopLen {
		num = loopLen
	}
	ag.equal(t, num)

	pool.Terminate()

	ag.end(t)
}

func TestNumGoroutine(t *testing.T) {
	runScheduler(t, 10, 10)
}

func TestNumGoroutineBlocking(t *testing.T) {
	runScheduler(t, 3, 7)
}

func TestFloodPoolAndTerminate(t *testing.T) {
	ag := newAssertGoroutine()

	pool := New(100, 5, 10)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			tInt(j).Shedule(pool)
		}
	}
	ag.equal(t, 100-5)

	pool.Terminate()
	pool.Terminate()

	ag.end(t)
}

func TestFloodPoolAndTerminate2(t *testing.T) {
	ag := newAssertGoroutine()

	pool := New(100, 5, 10)
	var wg sync.WaitGroup
	wg.Add(51)

	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			go func() {
				defer wg.Done()

				for j := 0; j < 40; j++ {
					tInt(j).Shedule(pool)
				}
			}()
		}
	}()

	pool.Terminate()
	pool.Schedule(nil)

	pool.Terminate()

	wg.Wait()
	ag.end(t)
}

func TestTimeout(t *testing.T) {
	ag := newAssertGoroutine()

	pool := New(1, 0, 0)

	pool.Schedule(func() {
		time.Sleep(2 * time.Second)
	})
	time.Sleep(time.Second)

	err := pool.ScheduleTimeout(100*time.Millisecond, func() {})
	assert.EqualError(t, err, ErrScheduleTimeout.Error())

	pool.Terminate()

	ag.end(t)
}

func BenchmarkScheduler(b *testing.B) {
	c := make(chan struct{})

	pool := New(128, 0, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Schedule(func() {
			c <- struct{}{}
		})
		<-c
	}
}

func BenchmarkGoroutine(b *testing.B) {
	c := make(chan struct{})

	for i := 0; i < b.N; i++ {
		go func() {
			c <- struct{}{}
		}()
		<-c
	}
}
