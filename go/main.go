package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	workerCount = 4
	taskCount   = 20
	outputFile  = "results.txt"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	queue := NewSharedTaskQueue(taskCount)
	results := make(chan Result, taskCount)
	var wg sync.WaitGroup

	// Producer: fill the queue, then close it so workers know when to stop
	// instead of blocking on GetTask forever.
	for i := 1; i <= taskCount; i++ {
		queue.AddTask(Task{ID: i, Payload: fmt.Sprintf("data-%d", i)})
	}
	queue.Close()

	wg.Add(workerCount)
	for i := 1; i <= workerCount; i++ {
		go worker(i, queue, results, &wg)
	}

	// Close the results channel once every worker has exited, so the range
	// loop below terminates instead of blocking forever.
	go func() {
		wg.Wait()
		close(results)
	}()

	collected := make([]Result, 0, taskCount)
	for r := range results {
		collected = append(collected, r)
	}

	if err := writeResults(outputFile, collected); err != nil {
		log.Printf("failed to write results: %v", err)
		os.Exit(1)
	}

	log.Printf("Processed %d / %d tasks successfully", len(collected), taskCount)
}

// writeResults persists results to disk, checking every error along the way
// (open, each write, and the final flush/close) and using defer to guarantee
// the file handle is released even if a write fails partway through.
func writeResults(path string, results []Result) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing output file: %w", closeErr)
		}
	}()

	writer := bufio.NewWriter(f)
	for _, r := range results {
		if _, werr := writer.WriteString(r.String() + "\n"); werr != nil {
			return fmt.Errorf("writing result %d: %w", r.TaskID, werr)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing output file: %w", err)
	}

	log.Printf("Wrote %d results to %s", len(results), path)
	return nil
}
