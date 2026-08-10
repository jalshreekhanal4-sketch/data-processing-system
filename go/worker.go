package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// worker pulls tasks from the shared queue until it is closed, "processes"
// each one (simulated by a short sleep), and forwards the outcome on the
// results channel. It runs as a goroutine; wg.Done() is deferred so the
// WaitGroup is decremented even if a panic is recovered below.
func worker(id int, queue *SharedTaskQueue, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	name := fmt.Sprintf("Worker-%d", id)
	log.Printf("%s starting", name)

	for {
		task, err := queue.GetTask()
		if err != nil {
			if errors.Is(err, ErrQueueClosed) {
				log.Printf("%s received shutdown signal", name)
				break
			}
			log.Printf("%s error retrieving task: %v", name, err)
			continue
		}

		output, err := processTask(task)
		if err != nil {
			log.Printf("%s failed to process task %d: %v", name, task.ID, err)
			continue
		}
		results <- Result{TaskID: task.ID, Worker: name, Output: output}
	}

	log.Printf("%s completed", name)
}

// processTask simulates computational work with a random delay and returns
// an error for malformed input, mirroring the kind of validation a real
// processing step (e.g. parsing a record) would need to perform.
func processTask(t Task) (string, error) {
	if strings.TrimSpace(t.Payload) == "" {
		return "", fmt.Errorf("task %d has an empty payload", t.ID)
	}
	time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)
	return strings.ToUpper(t.Payload) + "-PROCESSED", nil
}
