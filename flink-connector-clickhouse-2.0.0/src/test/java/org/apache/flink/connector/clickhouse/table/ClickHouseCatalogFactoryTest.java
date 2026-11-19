package org.apache.flink.connector.clickhouse.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.table.catalog.ClickHouseCatalog;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Port 1 throughout: nothing here reaches the network — that is {@code Catalog#open()}. */
class ClickHouseCatalogFactoryTest {

    private static final ClassLoader CLASS_LOADER = ClickHouseCatalogFactoryTest.class.getClassLoader();

    private static Map<String, String> options(String... extraKeysAndValues) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "clickhouse");
        options.put("url", "http://localhost:1");
        options.put("username", "default");
        for (int i = 0; i < extraKeysAndValues.length; i += 2) {
            options.put(extraKeysAndValues[i], extraKeysAndValues[i + 1]);
        }
        return options;
    }

    /** As {@code CREATE CATALOG ch WITH (...)} does it. */
    private static Catalog create(Map<String, String> options) {
        return FactoryUtil.createCatalog("ch", options, new Configuration(), CLASS_LOADER);
    }

    private static void assertRejected(Map<String, String> options, String... needles) {
        ValidationException e = assertThrows(ValidationException.class, () -> create(options));
        for (String needle : needles) {
            assertTrue(ExceptionUtils.findThrowableWithMessage(e, needle).isPresent(),
                    "Expected '" + needle + "' in " + ExceptionUtils.stringifyException(e));
        }
    }

    /** Both factories answer to 'clickhouse'; Flink tells them apart by the kind it asks for. */
    @Test void typeClickhouseIsDiscoveredBesideTheSinkFactory() {
        assertEquals(ClickHouseCatalogFactory.class,
                FactoryUtil.discoverFactory(CLASS_LOADER, CatalogFactory.class, "clickhouse").getClass());
        assertEquals(ClickHouseDynamicTableSinkFactory.class,
                FactoryUtil.discoverFactory(CLASS_LOADER, DynamicTableSinkFactory.class, "clickhouse").getClass());
    }

    @Test void theDefaultDatabaseIsClickHousesOwnUnlessSet() {
        Catalog catalog = create(options());
        assertEquals(ClickHouseCatalog.class, catalog.getClass());
        assertEquals("default", catalog.getDefaultDatabase());
        assertEquals("analytics", create(options("default-database", "analytics")).getDefaultDatabase());
    }

    @Test void urlAndUsernameAreRequired() {
        for (String required : List.of("url", "username")) {
            Map<String, String> options = options();
            options.remove(required);
            assertRejected(options, "required options are missing", required);
        }
    }

    /** The catalog fills these two in per table, so setting them on it would mean nothing. */
    @Test void databaseAndTableAreNotCatalogOptions() {
        assertRejected(options("database", "analytics"), "Unsupported options", "database");
        assertRejected(options("table", "events"), "Unsupported options", "table");
    }

    @Test void everySinkOptionIsAcceptedOnTheCatalog() {
        create(options(
                "password", "secret",
                "sink.buffer-flush.max-rows", "100",
                "sink.buffer-flush.interval", "1s",
                "sink.max-retries", "3",
                "sink.batch-failure-strategy", "drop-batch",
                "sink.timezone", "Asia/Tokyo",
                "sink.strict-numeric-mapping", "true",
                "sink.parallelism", "2",
                "clickhouse.client.socket_timeout", "30000",
                "clickhouse.server.async_insert", "1"));
    }

    /** A bad option must fail CREATE CATALOG as it would fail a table's planning, not wait for the first INSERT. */
    @Test void sinkOptionsAreCheckedAsTheSinkFactoryChecksThem() {
        assertRejected(options("url", "localhost:8123"), "'url'", "'localhost:8123'");
        assertRejected(options("sink.timezone", "Mars/Olympus_Mons"), "'sink.timezone'");
        assertRejected(options("sink.max-retries", "-3"), "'sink.max-retries'");
        assertRejected(options("sink.batch-failure-strategy", "retry-forever"), "'sink.batch-failure-strategy'");
        assertRejected(options("sink.max-buffered-requests", "400"), "'sink.max-buffered-requests' (400)");
        assertRejected(options("sink.strict-numeric-mapping", "maybe"), "sink.strict-numeric-mapping");
        assertRejected(options("clickhouse.client.connect_timeout", "1000"), "'clickhouse.client.connect_timeout'");
        assertRejected(options("clickhouse.client.database", "analytics"), "'clickhouse.client.database'");
        assertRejected(options("clickhouse.server.print_pretty_type_names", "1"),
                "'clickhouse.server.print_pretty_type_names'");
        assertRejected(options("sink.no-such-option", "1"), "Unsupported options", "sink.no-such-option");
    }

    /** Client.Builder.build() rejects these before any I/O, so open() reports an option error, not a failed read. */
    @Test void clientOptionsOnlyTheBuiltClientRejectsFailOpenAsOptionErrors() {
        ValidationException zone = assertThrows(ValidationException.class,
                create(options("clickhouse.client.use_time_zone", "Asia/Tokyo"))::open);
        assertTrue(zone.getMessage().contains("'clickhouse.client.*'"), zone.getMessage());
        assertTrue(zone.getMessage().contains("USE_TIME_ZONE"), zone.getMessage());
        assertFalse(zone.getMessage().contains("Could not read the metadata"), zone.getMessage());

        // An https endpoint builds the SSL context inside build(), which names the file only in a cause.
        ValidationException ssl = assertThrows(ValidationException.class,
                create(options("url", "https://localhost:1", "clickhouse.client.sslrootcert", "/nonexistent/ca.pem"))::open);
        assertTrue(ssl.getMessage().contains("'clickhouse.client.*'"), ssl.getMessage());
        assertTrue(ssl.getMessage().contains("/nonexistent/ca.pem"), ssl.getMessage());
        assertFalse(ssl.getMessage().contains("Could not read the metadata"), ssl.getMessage());
    }

    @Test void tablesTakeEveryCatalogOptionButTheDefaultDatabase() {
        Map<String, String> catalogOptions = options(
                "default-database", "analytics", "sink.max-retries", "3", "clickhouse.server.async_insert", "1");
        // FactoryUtil strips the discovery key before the factory sees the options.
        catalogOptions.remove("type");

        assertEquals(
                Map.of("connector", "clickhouse", "url", "http://localhost:1", "username", "default",
                        "sink.max-retries", "3", "clickhouse.server.async_insert", "1"),
                ClickHouseCatalogFactory.tableOptions(catalogOptions));
    }
}
