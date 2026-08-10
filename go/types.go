package main

import "fmt"

// Task is a single unit of work retrieved from the shared queue.
type Task struct {
	ID      int
	Payload string
}

// Result is the outcome of a worker processing a Task.
type Result struct {
	TaskID int
	Worker string
	Output string
}

func (r Result) String() string {
	return fmt.Sprintf("Result{taskId=%d, worker=%q, output=%q}", r.TaskID, r.Worker, r.Output)
}
