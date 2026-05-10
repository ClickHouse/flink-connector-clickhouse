package com.example;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.List;

/**
 * Emits all elements once, then idles until cancelled. Same purpose as the
 * Flink 2.0 version of this class; the package of {@code RichParallelSourceFunction}
 * differs (no {@code .legacy.}) and the runtime context API is the pre-1.20 form.
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
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

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
