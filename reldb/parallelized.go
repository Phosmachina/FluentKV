package fluentkv

import (
	"fmt"
	"sync"
)

var MaxTasks = 50

type Task func()

type TaskPool struct {
	tasks   chan Task
	wg      sync.WaitGroup
	workers int
	quit    chan bool
	mu      sync.Mutex
}

func NewTaskPool() *TaskPool {
	pool := &TaskPool{
		tasks: make(chan Task, MaxTasks), // A buffered channel
		wg:    sync.WaitGroup{},
		quit:  make(chan bool)}
	return pool
}

func (tp *TaskPool) worker() {

	for {
		select {
		case task := <-tp.tasks:
			defer func() {
				a := recover()
				if a != nil {
					fmt.Println("[WORKER] Panic:", a)
					tp.wg.Done()
				}
			}()
			task()
			tp.wg.Done()
		case <-tp.quit:
			tp.mu.Lock()
			tp.workers--
			tp.mu.Unlock()
			return
		}
	}
}

func (tp *TaskPool) AddTask(task Task) {
	tp.mu.Lock()
	if tp.workers < MaxTasks {
		tp.workers++
		go tp.worker()
	}
	tp.mu.Unlock()
	tp.wg.Add(1)
	tp.tasks <- task
}

func (tp *TaskPool) Close() {

	tp.wg.Wait()

	tp.mu.Lock()
	workers := tp.workers
	tp.mu.Unlock()

	for i := 0; i < workers; i++ {
		tp.quit <- true
	}

	close(tp.tasks)
}
