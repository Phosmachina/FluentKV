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
	//fmt.Println("[WORKER] Created...")

	for {
		select {
		case task := <-tp.tasks:
			//fmt.Println("[WORKER] Start a task.")
			defer func() {
				a := recover()
				if a != nil {
					fmt.Println("[WORKER] Panic:", a)
					tp.wg.Done()
				}
			}()
			task()
			//fmt.Println("[WORKER] Task is done.")
			tp.wg.Done()
		case <-tp.quit:
			//fmt.Println("[WORKER] Receive quit signal.")
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
		//fmt.Println("[ADD] New worker created. Count:", tp.workers)
		go tp.worker()
	}
	tp.mu.Unlock()
	tp.wg.Add(1)
	//fmt.Println("[ADD] Task is buffered.")
	tp.tasks <- task
}

func (tp *TaskPool) Close() {
	fmt.Println("[CLOSED] Wait all tasks are finished.")
	tp.wg.Wait()
	fmt.Println("[CLOSED] Send close to workers.")
	tp.mu.Lock()
	workers := tp.workers
	tp.mu.Unlock()
	for i := 0; i < workers; i++ {
		//fmt.Println("[CLOSED] Send quit to worker:", i)
		tp.quit <- true
	}
	fmt.Println("[CLOSED] Now close tasks channel.")
	close(tp.tasks)
	fmt.Println("[CLOSED] end.")
}
