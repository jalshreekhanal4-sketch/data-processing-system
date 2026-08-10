# Data Processing System

A multi-threaded/multi-goroutine data processing system implemented twice —
once in Java, once in Go — demonstrating a shared task queue, a pool of
worker threads/goroutines, synchronized access to shared resources, and
exception/error handling. Both implementations produce the same result:
20 tasks are distributed across 4 workers, "processed" with a simulated
delay, and the results are written to `results.txt`.

## Java (`java/`)

Uses a `ReentrantLock` + `Condition`-backed shared queue, an
`ExecutorService` fixed thread pool for worker management, and
`try/catch` for `InterruptedException` and `IOException`. Logging is done
via `java.util.logging`.

```bash
cd java
javac -d out src/dps/*.java
java -cp out dps.Main
cat results.txt
```

## Go (`go/`)

Uses a buffered channel (wrapped in a small `SharedTaskQueue` type exposing
`AddTask`/`GetTask`) as the concurrency-safe queue, goroutines as workers,
a `sync.WaitGroup` to track completion, and explicit error checking with
`defer` for file cleanup. Logging is done via the standard `log` package.

```bash
cd go
go build -o dps .
./dps
cat results.txt
```

Run with the race detector to verify there are no data races:

```bash
go run -race .
```

## Design notes

- **Termination**: Java uses a "poison pill" sentinel task (one per worker)
  pushed onto the queue after all real work so every worker gets an
  unambiguous, non-blocking shutdown signal. Go closes the task channel
  after the producer finishes; `range`/receive-with-`ok` on a closed,
  drained channel returns immediately, giving the same effect idiomatically.
- **No deadlocks**: neither worker ever holds more than one lock/channel at
  a time, and both queues block only while genuinely empty — never while
  holding a lock other threads/goroutines need.
- **Results collection**: Java uses a `CopyOnWriteArrayList` (safe to read
  after all workers finish, cheap under the sequential append pattern of
  writes here); Go uses a buffered `Result` channel drained by the main
  goroutine after `wg.Wait()`.
