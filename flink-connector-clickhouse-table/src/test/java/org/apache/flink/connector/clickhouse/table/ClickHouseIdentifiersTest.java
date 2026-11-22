package org.apache.flink.connector.clickhouse.table;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClickHouseIdentifiersTest {

    @Test
    void plainNamesAreWrappedInBackquotes() {
        assertEquals("`events`", ClickHouseIdentifiers.quote("events"));
        assertEquals("`_events_2`", ClickHouseIdentifiers.quote("_events_2"));
    }

    @Test
    void namesThatNeedQuotingStayOneIdentifier() {
        assertEquals("`my-table`", ClickHouseIdentifiers.quote("my-table"));
        assertEquals("`2024_events`", ClickHouseIdentifiers.quote("2024_events"));
        assertEquals("`my table`", ClickHouseIdentifiers.quote("my table"));
        // A dot is part of the table name here; the database travels separately.
        assertEquals("`a.b`", ClickHouseIdentifiers.quote("a.b"));
    }

    @Test
    void backquotesAndBackslashesInsideAreEscaped() {
        assertEquals("`a\\`b`", ClickHouseIdentifiers.quote("a`b"));
        assertEquals("`a\\\\b`", ClickHouseIdentifiers.quote("a\\b"));
        // The backslash is escaped first, so an escaped backquote is not undone.
        assertEquals("`a\\\\\\`b`", ClickHouseIdentifiers.quote("a\\`b"));
    }

    @Test
    void nullIsRejected() {
        assertThrows(NullPointerException.class, () -> ClickHouseIdentifiers.quote(null));
    }
}
