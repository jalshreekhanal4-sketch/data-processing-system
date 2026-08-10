package dps;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded-free, thread-safe FIFO queue shared by all worker threads.
 *
 * A {@link ReentrantLock} paired with a {@link Condition} is used instead of
 * a plain {@code synchronized} block so that {@code getTask()} can block
 * without holding the lock while idle, and so waiting threads are woken up
 * (via {@code signalAll}) rather than left polling. This is the same
 * producer/consumer pattern {@code java.util.concurrent.BlockingQueue}
 * implements internally.
 */
public class SharedTaskQueue {

    private final Queue<Task> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    /** Adds a task to the tail of the queue and wakes one waiting consumer. */
    public void addTask(Task task) {
        lock.lock();
        try {
            queue.offer(task);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes and returns the task at the head of the queue, blocking until
     * one is available.
     *
     * @throws InterruptedException if the calling thread is interrupted
     *         while waiting; callers must decide whether to shut down.
     */
    public Task getTask() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }
}
