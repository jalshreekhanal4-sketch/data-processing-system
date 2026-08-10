package dps;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

/**
 * Shared sink for completed work. Backed by {@link CopyOnWriteArrayList},
 * which is appropriate here because writes (one per completed task) are
 * infrequent relative to the processing delay, and it never requires
 * external locking for the append/read pattern used by the workers and the
 * final report.
 */
public class ResultsCollector {

    private static final Logger LOGGER = Logger.getLogger(ResultsCollector.class.getName());

    private final List<Result> results = new CopyOnWriteArrayList<>();

    public void add(Result result) {
        results.add(result);
    }

    public List<Result> snapshot() {
        return Collections.unmodifiableList(results);
    }

    /**
     * Persists all collected results to disk. IOException is caught here
     * (rather than propagated) so that a file-system problem is logged and
     * surfaced without crashing the whole application after all worker
     * threads have already completed their in-memory work.
     */
    public void writeToFile(Path outputPath) {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            for (Result result : results) {
                writer.write(result.toString());
                writer.newLine();
            }
            LOGGER.info("Wrote " + results.size() + " results to " + outputPath.toAbsolutePath());
        } catch (IOException e) {
            LOGGER.severe("Failed to write results to " + outputPath + ": " + e.getMessage());
        }
    }
}
