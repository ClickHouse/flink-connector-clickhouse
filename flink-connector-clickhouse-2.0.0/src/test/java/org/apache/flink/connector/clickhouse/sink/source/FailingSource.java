package org.apache.flink.connector.clickhouse.sink.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * A source that emits all records, waits for a checkpoint to complete, then fails.
 * After restart from checkpoint, it emits nothing — proving that any data
 * delivered to ClickHouse after the restart came from checkpoint state.
 *
 * Accepts a {@link RowCountChecker} to query ClickHouse row count at checkpoint time.
 * Two modes:
 * - requireAllBuffered=true: fails if any rows already in ClickHouse (all must come from rehydration)
 * - requireAllBuffered=false: allows partial flush, records the count at checkpoint time
 */
public class FailingSource<T extends Serializable> extends RichParallelSourceFunction<T>
        implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(FailingSource.class);

    @FunctionalInterface
    public interface RowCountChecker extends Serializable {
        int getRowCount() throws Exception;
    }

    private final List<T> elements;
    private final RowCountChecker rowCountChecker;
    private final boolean requireAllBuffered;
    private volatile boolean running = true;
    private volatile boolean hasFailed = false;
    private volatile boolean checkpointCompleted = false;
    private volatile boolean allEmitted = false;

    // Tracks rows already in ClickHouse at checkpoint time (for partial flush tests)
    private volatile int rowsAtCheckpointTime = 0;

    private transient ListState<Boolean> hasFailedState;

    public FailingSource(List<T> elements) {
        this(elements, null, true);
    }

    public FailingSource(List<T> elements, RowCountChecker rowCountChecker) {
        this(elements, rowCountChecker, true);
    }

    public FailingSource(List<T> elements, RowCountChecker rowCountChecker, boolean requireAllBuffered) {
        this.elements = elements;
        this.rowCountChecker = rowCountChecker;
        this.requireAllBuffered = requireAllBuffered;
    }

    public int getRowsAtCheckpointTime() {
        return rowsAtCheckpointTime;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        if (hasFailed) {
            LOG.info("FailingSource: restarted after failure, not emitting any records");
            while (running) {
                Thread.sleep(100);
            }
            return;
        }

        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

        synchronized (ctx.getCheckpointLock()) {
            for (int i = subtaskIndex; i < elements.size(); i += numSubtasks) {
                ctx.collect(elements.get(i));
            }
            allEmitted = true;
        }

        LOG.info("FailingSource: all records emitted, waiting for checkpoint to complete");

        while (!checkpointCompleted && running) {
            Thread.sleep(100);
        }

        if (checkpointCompleted) {
            LOG.info("FailingSource: checkpoint completed, throwing exception to trigger restart");
            throw new RuntimeException("Intentional failure after checkpoint to test state restore");
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (allEmitted && !hasFailed) {
            if (rowCountChecker != null) {
                rowsAtCheckpointTime = rowCountChecker.getRowCount();
                LOG.info("FailingSource: checkpoint {} completed, {} rows already in ClickHouse",
                        checkpointId, rowsAtCheckpointTime);

                if (requireAllBuffered && rowsAtCheckpointTime > 0) {
                    throw new RuntimeException(String.format(
                            "FailingSource: checkpoint %d completed but %d rows already in ClickHouse. "
                            + "Data was flushed before checkpoint — test cannot verify rehydration.",
                            checkpointId, rowsAtCheckpointTime));
                }

                if (!requireAllBuffered && rowsAtCheckpointTime == 0) {
                    throw new RuntimeException(String.format(
                            "FailingSource: checkpoint %d completed with 0 rows in ClickHouse. "
                            + "Expected partial flush — increase record count or reduce batch size.",
                            checkpointId));
                }
            }

            checkpointCompleted = true;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        hasFailedState.clear();
        hasFailedState.add(true);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        hasFailedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("has-failed", Boolean.class));

        if (context.isRestored()) {
            for (Boolean failed : hasFailedState.get()) {
                if (failed != null && failed) {
                    hasFailed = true;
                    LOG.info("FailingSource: restored from checkpoint, marking as already failed");
                }
            }
        }
    }
}
