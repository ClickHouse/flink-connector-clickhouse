package com.clickhouse.config;

import java.io.Serializable;

public enum BatchFailureStrategy implements Serializable {
    DROP_BATCH,
    STOP_FLINK,
    // TODO: once we implement this option let's add this option -> MOVE_TO_DLQ,
}
