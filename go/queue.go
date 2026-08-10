package main

import "errors"

// ErrQueueClosed is returned by GetTask once the queue has been closed and
// fully drained, signalling a worker that no more work will ever arrive.
var ErrQueueClosed = errors.New("task queue is closed")

// SharedTaskQueue is a concurrency-safe FIFO queue of Tasks shared by all
// worker goroutines. Rather than hand-rolling a mutex-protected slice, it
// wraps a buffered channel: channels are Go's built-in, race-free
// producer/consumer primitive, and the buffer lets the producer stay ahead
// of the workers without blocking on every single AddTask call.
type SharedTaskQueue struct {
	tasks chan Task
}

// NewSharedTaskQueue creates a queue with the given buffer capacity.
func NewSharedTaskQueue(capacity int) *SharedTaskQueue {
	return &SharedTaskQueue{tasks: make(chan Task, capacity)}
}

// AddTask enqueues a task. It blocks only if the internal buffer is full,
// which naturally throttles a producer that outpaces the workers.
func (q *SharedTaskQueue) AddTask(t Task) {
	q.tasks <- t
}

// Close signals that no further tasks will be added. Must be called exactly
// once, after all AddTask calls have completed.
func (q *SharedTaskQueue) Close() {
	close(q.tasks)
}

// GetTask blocks until a task is available, returning ErrQueueClosed once
// the queue has been closed and drained so callers can exit their loop
// instead of blocking forever.
func (q *SharedTaskQueue) GetTask() (Task, error) {
	task, ok := <-q.tasks
	if !ok {
		return Task{}, ErrQueueClosed
	}
	return task, nil
}
