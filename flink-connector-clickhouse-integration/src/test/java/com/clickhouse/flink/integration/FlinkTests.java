package com.clickhouse.flink.integration;

import com.clickhouse.flink.Cluster;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FlinkTests {
    static int NUMBERS_OF_RECORDS = 100000;
    static String flinkVersion = "latest";
    static ExampleLang exampleLang = ExampleLang.JAVA;

    enum ExampleLang { JAVA, SCALA }

    @BeforeAll
    public static void setUp() throws Exception {
        flinkVersion = (System.getenv("FLINK_VERSION") != null ? System.getenv("FLINK_VERSION") : "latest");
        exampleLang = "scala".equalsIgnoreCase(System.getProperty("example.lang", "java"))
                ? ExampleLang.SCALA
                : ExampleLang.JAVA;
        System.out.println("FLINK_VERSION: " + flinkVersion);
        System.out.println("EXAMPLE_LANG: " + exampleLang);
        ClickHouseServerForTests.setUp(false);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        ClickHouseServerForTests.tearDown();
    }

    private String getRoot() throws Exception {
        String PWD = System.getenv("PWD");
        if (PWD != null)
            return PWD;
        String GITHUB_WORKSPACE = System.getenv("GITHUB_WORKSPACE");
        if (GITHUB_WORKSPACE != null)
            return GITHUB_WORKSPACE;
        else
            throw new RuntimeException("Can not get root path");
    }

    private String exampleSubFolder(String flinkVersion) {
        if (flinkVersion.equalsIgnoreCase("latest") || flinkVersion.startsWith("2."))
            return "flink-v2";
        return "flink-v1.7";
    }

    private String jarLocation(String root, String exampleSubFolder) {
        if (exampleLang == ExampleLang.SCALA)
            return String.format("%s/examples/sbt/%s/covid/target/scala-2.12/covid.jar", root, exampleSubFolder);
        return String.format("%s/examples/maven/%s/covid/target/covid-1.0-SNAPSHOT.jar", root, exampleSubFolder);
    }

    private String mainClass() {
        if (exampleLang == ExampleLang.SCALA)
            return "Main";
        return "com.clickhouse.example.covid.DataStreamJob";
    }

    private String getResourcePath(String resourceName) throws URISyntaxException {
        URI resourceUri = getClass().getResource("/data/" + resourceName).toURI();
        Path resourcePath = Paths.get(resourceUri);
        String dataFileLocation = resourcePath.getParent().toString() + "/";
        return dataFileLocation;
    }

    @Test
    void testFlinkCluster() throws Exception {
        String root = getRoot();
        String exampleSubFolder = exampleSubFolder(flinkVersion);
        String jarLocation = jarLocation(root, exampleSubFolder);
        System.out.println("exampleSubFolder: " + exampleSubFolder);
        String dataFile = "100k_epidemiology.csv.gz";
        String tableName = "covid";

        String dataFileLocation = getResourcePath(dataFile);
        // Since we have in docker communication
        String clickHouseURL = ClickHouseServerForTests.getURLForCluster();
        String username = ClickHouseServerForTests.getUsername();
        String password = ClickHouseServerForTests.getPassword();
        String database = ClickHouseServerForTests.getDatabase();

        // create table
        String dropTable = String.format("DROP TABLE IF EXISTS `%s`.`%s`", database, tableName);
        ClickHouseServerForTests.executeSql(dropTable);
        // create table
        String tableSql = "CREATE TABLE `" + database + "`.`" + tableName + "` (" +
                "date Date," +
                "location_key LowCardinality(String)," +
                "new_confirmed Int32," +
                "new_deceased Int32," +
                "new_recovered Int32," +
                "new_tested Int32," +
                "cumulative_confirmed Int32," +
                "cumulative_deceased Int32," +
                "cumulative_recovered Int32," +
                "cumulative_tested Int32" +
                ") " +
                "ENGINE = MergeTree " +
                "ORDER BY (location_key, date); ";

        ClickHouseServerForTests.executeSql(tableSql);

        // start on demand flink cluster
        Network network = ClickHouseServerForTests.getNetwork();

        Cluster.Builder builder = new Cluster.Builder()
                .withTaskManagers(3)
                .withNetwork(network)
                .withDataFile(dataFileLocation, dataFile, "/tmp")
                .withFlinkVersion(flinkVersion);

        Cluster cluster = builder.build();
        String jarId = cluster.uploadJar(jarLocation);
        List<String> jars = cluster.listAllJars();
        String jobId = cluster.runJob(jars.get(0),
                mainClass(),
                1,
                "-input",
                "/tmp/" + dataFile,
                "-url",
                clickHouseURL,
                "-username",
                username,
                "-password",
                password,
                "-database",
                database,
                "-table",
                tableName
                );
        String state = cluster.jobStatus(jobId);
        while (state.equalsIgnoreCase("RUNNING")) {
            state = cluster.jobStatus(jobId);
            int count = ClickHouseServerForTests.countRows(tableName);
            if (count ==  NUMBERS_OF_RECORDS)
                break;
            Thread.sleep(2000);
        }
        int count = ClickHouseServerForTests.countRows(tableName);
        Assertions.assertEquals(NUMBERS_OF_RECORDS, count);
        // destroy cluster
        cluster.tearDown();
    }

    private String mapEvolutionJar(String root, String exampleSubFolder) {
        return String.format(
                "%s/examples/maven/%s/map-evolution/target/map-evolution-1.0-SNAPSHOT.jar",
                root, exampleSubFolder);
    }

    /**
     * Production schema-evolution flow under the v0.2.0 Map-based payload design:
     *
     * <ol>
     *   <li><b>Build A</b> runs with {@code MapEvolutionMapperA} (columns
     *       {@code a Int32, b String}). It emits 100 records but doesn't flush —
     *       the sink's {@code maxTimeInBufferMS = 600_000} keeps them buffered.</li>
     *   <li>A savepoint is taken. The Map-based state checkpointed is
     *       {@code {a: int, b: string}} per row — no {@code c} key.</li>
     *   <li>The table is altered: {@code ADD COLUMN c Nullable(String) DEFAULT 'omg'}.</li>
     *   <li><b>Build B</b> restarts from the savepoint with {@code MapEvolutionMapperB}
     *       (which adds {@code c String}). It also emits 100 fresh records carrying
     *       {@code c = "extra-{i}"}.</li>
     *   <li>The restored entries have no {@code c} key in their Map — Build B's mapper
     *       reads {@code Map.get("c") == null} and writes NULL on a Nullable column.
     *       The fresh Build B entries carry actual values.</li>
     * </ol>
     *
     * <p>Asserts the resulting table has 200 rows total: 100 from Build A's restored
     * checkpoint state (with {@code c IS NULL}) and 100 from fresh Build B emission
     * (with {@code c} populated).
     *
     * <p>Both builds share the same JAR; the {@code -build A|B} argument selects the
     * mapper. That's the whole point of the design: schema evolution lives in user
     * code (DataMapper), not in Flink's {@code TypeSerializerSnapshot}.
     *
     * <p>Skipped when running in Scala mode — the map-evolution example is Java-only.
     */
    @Test
    void testSchemaEvolution() throws Exception {
        if (exampleLang == ExampleLang.SCALA) return;

        String root = getRoot();
        String exampleSubFolder = exampleSubFolder(flinkVersion);
        String jarPath = mapEvolutionJar(root, exampleSubFolder);

        File jar = new File(jarPath);
        if (!jar.exists()) {
            System.out.println("Skipping testSchemaEvolution — example JAR not built. "
                    + "Run mvn -q clean package in examples/maven/" + exampleSubFolder
                    + "/map-evolution");
            return;
        }

        String tableName = "map_evolution";
        String clickHouseURL = ClickHouseServerForTests.getURLForCluster();
        String username = ClickHouseServerForTests.getUsername();
        String password = ClickHouseServerForTests.getPassword();
        String database = ClickHouseServerForTests.getDatabase();

        // 1. Build-A-shape table: just (a, b).
        ClickHouseServerForTests.executeSql(
                String.format("DROP TABLE IF EXISTS `%s`.`%s`", database, tableName));
        ClickHouseServerForTests.executeSql(
                String.format("CREATE TABLE `%s`.`%s` (a Int32, b String) "
                        + "ENGINE = MergeTree ORDER BY a", database, tableName));

        // 2. Flink cluster with a host-mounted shared state dir so savepoints
        //    survive across the two jobs.
        Network network = ClickHouseServerForTests.getNetwork();
        Path sharedStateDir = Files.createTempDirectory("flink-map-evo-state-");
        sharedStateDir.toFile().setWritable(true, false);
        sharedStateDir.toFile().setReadable(true, false);
        sharedStateDir.toFile().setExecutable(true, false);

        Cluster cluster = new Cluster.Builder()
                .withTaskManagers(1)
                .withNetwork(network)
                .withSharedStateVolume(sharedStateDir.toAbsolutePath().toString())
                .withFlinkVersion(flinkVersion)
                .build();

        try {
            // 3. Upload + run Build A.
            String jarId = cluster.uploadJar(jarPath);
            String buildAJobId = cluster.runJob(jarId, "com.example.MapEvolutionJob", 1,
                    "-url", clickHouseURL,
                    "-username", username,
                    "-password", password,
                    "-database", database,
                    "-table", tableName,
                    "-build", "A",
                    "-records", "100",
                    "-idStart", "0");

            // 4. Wait for source emission and at least one checkpoint to land.
            //    The sink keeps records buffered (maxTimeInBufferMS = 600_000).
            Thread.sleep(8000);
            Assertions.assertEquals(0, ClickHouseServerForTests.countRows(tableName),
                    "Build-A records should still be in writer buffer at savepoint time, not in ClickHouse");

            // 5. Take savepoint, then cancel Build A.
            String savepointPath = cluster.triggerSavepoint(buildAJobId, Cluster.CONTAINER_SAVEPOINT_DIR);
            Assertions.assertNotNull(savepointPath, "Savepoint trigger should return a location");
            cluster.cancelJob(buildAJobId);
            Assertions.assertEquals(0, ClickHouseServerForTests.countRows(tableName),
                    "Build-A records should still be in writer buffer at savepoint time, not in ClickHouse");

            // 6. Schema migration on the table.
            ClickHouseServerForTests.executeSql(String.format(
                    "ALTER TABLE `%s`.`%s` ADD COLUMN c Nullable(String) DEFAULT 'omg'",
                    database, tableName));

            // 7. Run Build B from the Build-A savepoint. The restored Map entries
            //    have no "c" key; Build B's mapper writes null for them. Fresh
            //    Build B entries (idStart=100) carry c = "extra-{i}".
            String buildBJobId = cluster.runJobFromSavepoint(jarId, "com.example.MapEvolutionJob", 1,
                    savepointPath,
                    "-url", clickHouseURL,
                    "-username", username,
                    "-password", password,
                    "-database", database,
                    "-table", tableName,
                    "-build", "B",
                    "-records", "100",
                    "-idStart", "100");
            Assertions.assertNotNull(buildBJobId, "Build-B job submission should succeed");

            // 8. Wait for restored Build-A records (null c) + fresh Build-B records
            //    (populated c) to flush.
            int expectedTotal = 200;
            int rows = 0;
            for (int i = 0; i < 60; i++) {
                Thread.sleep(1000);
                rows = ClickHouseServerForTests.countRows(tableName);
                if (rows == expectedTotal) break;
            }

            cluster.cancelJob(buildBJobId);

            Assertions.assertEquals(expectedTotal, rows,
                    "After schema-evolved restore, all Build-A buffered records must arrive with "
                            + "c=NULL alongside the 100 freshly emitted Build-B records.");

            int buildAOrigin = ClickHouseServerForTests.countRowsWhere(tableName, "c IS NULL");
            int buildBOrigin = ClickHouseServerForTests.countRowsWhere(tableName,
                    "c IS NOT NULL AND c LIKE 'extra-%'");
            Assertions.assertEquals(100, buildAOrigin,
                    "Expected 100 rows from Build-A origin — restored Map had no 'c' key, "
                            + "Build B's mapper writes null on the Nullable column.");
            Assertions.assertEquals(100, buildBOrigin,
                    "Expected 100 rows from fresh Build-B emission (c = 'extra-{i}').");
        } finally {
            cluster.tearDown();
        }
    }
}
