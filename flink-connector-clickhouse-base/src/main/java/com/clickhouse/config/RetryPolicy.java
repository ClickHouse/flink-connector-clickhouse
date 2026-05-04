package com.clickhouse.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * Retry policy configuration for ClickHouse operations.
 *
 * <p>This class defines how many times an operation should be retried before giving up.
 * It supports both unlimited retries and a fixed maximum number of retries.
 *
 * <p>This class is immutable and thread-safe.
 */
public final class RetryPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Null means unlimited retries, otherwise the maximum number of retry attempts. */
    private final Integer maxRetries;

    /**
     * Creates a new retry policy.
     *
     * @param maxRetries maximum number of retries, or null for unlimited retries
     */
    private RetryPolicy(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Creates a retry policy with unlimited retries.
     *
     * @return a retry policy that never gives up
     */
    public static RetryPolicy forever() {
        return new RetryPolicy(null);
    }

    /**
     * Creates a retry policy with a limited number of retries.
     *
     * @param maxRetries maximum number of retry attempts, must be non-negative
     * @return a retry policy with the specified maximum number of retries
     * @throws IllegalArgumentException if maxRetries is negative
     */
    public static RetryPolicy limited(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Maximum retries must be non-negative, but was: " + maxRetries);
        }
        return new RetryPolicy(maxRetries);
    }

    /**
     * Checks if this retry policy allows unlimited retries.
     *
     * @return true if retries are unlimited, false otherwise
     */
    public boolean isForever() {
        return maxRetries == null;
    }

    /**
     * Gets the maximum number of retries.
     *
     * @return the maximum number of retries
     * @throws IllegalStateException if this policy allows unlimited retries
     */
    public int getValue() {
        if (maxRetries == null) {
            throw new IllegalStateException("Cannot get value of unlimited retry policy");
        }
        return maxRetries;
    }

    /**
     * Gets the maximum number of retries, or a default value if this policy allows unlimited retries.
     *
     * @param defaultValue the default value to return when retries are unlimited
     * @return the maximum number of retries, or the default value if unlimited
     */
    public int getValueOrDefault(int defaultValue) {
        return maxRetries != null ? maxRetries : defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryPolicy that = (RetryPolicy) o;
        return Objects.equals(maxRetries, that.maxRetries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxRetries);
    }

    @Override
    public String toString() {
        return isForever() ? "RetryPolicy{forever}" : "RetryPolicy{maxRetries=" + maxRetries + "}";
    }
}
