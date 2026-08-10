package dps;

/** The outcome of processing a single {@link Task}. */
public final class Result {

    private final int taskId;
    private final String workerName;
    private final String output;

    public Result(int taskId, String workerName, String output) {
        this.taskId = taskId;
        this.workerName = workerName;
        this.output = output;
    }

    @Override
    public String toString() {
        return "Result{taskId=" + taskId + ", worker='" + workerName
                + "', output='" + output + "'}";
    }
}
