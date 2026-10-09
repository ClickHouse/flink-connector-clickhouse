package org.apache.flink.connector.clickhouse.sink;

import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;
import org.apache.flink.connector.base.sink.writer.strategy.ResultInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link RateLimitingStrategy} that never blocks but counts registration calls, used to prove
 * that a caller-supplied strategy is actually wired into {@link ClickHouseAsyncWriter} rather
 * than the sink silently falling back to its default congestion-control strategy.
 *
 * <p>Counters are static: the embedded test cluster runs task threads in the same JVM as the
 * test, so a static counter is visible to the assertion after the job completes.
 */
public class TrackingRateLimitingStrategy implements RateLimitingStrategy {
    static final AtomicInteger inFlightRegistrations = new AtomicInteger();
    static final AtomicInteger completedRegistrations = new AtomicInteger();

    private final int maxBatchSize;

    public TrackingRateLimitingStrategy(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    static void reset() {
        inFlightRegistrations.set(0);
        completedRegistrations.set(0);
    }

    @Override
    public void registerInFlightRequest(RequestInfo requestInfo) {
        inFlightRegistrations.incrementAndGet();
    }

    @Override
    public void registerCompletedRequest(ResultInfo resultInfo) {
        completedRegistrations.incrementAndGet();
    }

    @Override
    public boolean shouldBlock(RequestInfo requestInfo) {
        return false;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }
}
