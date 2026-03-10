package com.clickhouse.config;

import java.io.Serializable;

public final class RetryPolicy implements Serializable {

    private final Integer maxRetries;

    private RetryPolicy(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public static RetryPolicy forever()       { return new RetryPolicy(null); }
    public static RetryPolicy limited(int v)   { return new RetryPolicy(v); }

    public boolean isForever()                 { return maxRetries != null; }
    public int getValue()                      { if (maxRetries == null) throw new IllegalStateException(); return maxRetries; }
    public int getValueOrDefault(int def)      { return maxRetries != null ? maxRetries : def; }
}
