package dps;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Entry point: builds a shared task queue, populates it with sample work,
 * fans it out to a fixed pool of worker threads via an {@link ExecutorService},
 * waits for graceful completion, then writes the aggregated results to disk.
 */
public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    private static final int WORKER_COUNT = 4;
    private static final int TASK_COUNT = 20;

    public static void main(String[] args) {
        SharedTaskQueue taskQueue = new SharedTaskQueue();
        ResultsCollector results = new ResultsCollector();
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);

        // Produce the work items up front.
        for (int i = 1; i <= TASK_COUNT; i++) {
            taskQueue.addTask(new Task(i, "data-" + i));
        }
        // One poison pill per worker guarantees every thread gets a distinct
        // shutdown signal and none blocks forever waiting on an empty queue.
        for (int i = 0; i < WORKER_COUNT; i++) {
            taskQueue.addTask(Task.POISON_PILL);
        }

        for (int i = 1; i <= WORKER_COUNT; i++) {
            executor.submit(new Worker("Worker-" + i, taskQueue, results));
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warning("Workers did not finish within the timeout; forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe("Main thread interrupted while awaiting worker completion: " + e.getMessage());
            executor.shutdownNow();
        }

        results.writeToFile(Path.of("results.txt"));
        LOGGER.info("Processed " + results.snapshot().size() + " / " + TASK_COUNT + " tasks successfully");
    }
}
