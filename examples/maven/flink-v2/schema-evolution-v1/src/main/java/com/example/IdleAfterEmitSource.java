package com.example;

import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.List;

/**
 * Emits all elements once, then idles until cancelled. Used so the job stays
 * alive long enough for the upgrade workflow to take a savepoint.
 */
public class IdleAfterEmitSource<T extends Serializable> extends RichParallelSourceFunction<T> {
    private static final long serialVersionUID = 1L;

    private final List<T> elements;
    private volatile boolean running = true;

    public IdleAfterEmitSource(List<T> elements) {
        this.elements = elements;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

        synchronized (ctx.getCheckpointLock()) {
            for (int i = subtaskIndex; i < elements.size(); i += numSubtasks) {
                ctx.collect(elements.get(i));
            }
        }

        while (running) {
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
