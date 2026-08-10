package dps;

/**
 * A single unit of work to be processed by a worker thread.
 * A POISON_PILL task (id == -1) is used to signal a worker to stop
 * gracefully once no more real work remains in the queue.
 */
public final class Task {

    public static final Task POISON_PILL = new Task(-1, null);

    private final int id;
    private final String payload;

    public Task(int id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    public int getId() {
        return id;
    }

    public String getPayload() {
        return payload;
    }

    public boolean isPoisonPill() {
        return id == -1;
    }

    @Override
    public String toString() {
        return "Task{id=" + id + ", payload='" + payload + "'}";
    }
}
