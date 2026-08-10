package dps;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * A worker thread: pulls tasks from the shared queue until it receives the
 * poison pill, "processes" each one (simulated by a short sleep), and
 * records the outcome in the shared {@link ResultsCollector}.
 */
public class Worker implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Worker.class.getName());

    private final String name;
    private final SharedTaskQueue taskQueue;
    private final ResultsCollector results;

    public Worker(String name, SharedTaskQueue taskQueue, ResultsCollector results) {
        this.name = name;
        this.taskQueue = taskQueue;
        this.results = results;
    }

    @Override
    public void run() {
        LOGGER.info(name + " starting");
        try {
            while (true) {
                Task task = taskQueue.getTask();
                if (task.isPoisonPill()) {
                    LOGGER.info(name + " received shutdown signal");
                    break;
                }
                processTask(task);
            }
            LOGGER.info(name + " completed");
        } catch (InterruptedException e) {
            // Restore the interrupt status so callers further up the stack
            // (e.g. the executor) can also observe and react to it.
            Thread.currentThread().interrupt();
            LOGGER.warning(name + " was interrupted while waiting for work: " + e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.severe(name + " encountered an unexpected error: " + e.getMessage());
        }
    }

    private void processTask(Task task) {
        try {
            LOGGER.info(name + " processing " + task);
            // Simulate variable computational work.
            Thread.sleep(ThreadLocalRandom.current().nextInt(100, 500));
            String output = task.getPayload().toUpperCase() + "-PROCESSED";
            results.add(new Result(task.getId(), name, output));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warning(name + " interrupted mid-task on " + task + ": " + e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.severe(name + " failed to process " + task + ": " + e.getMessage());
        }
    }
}
