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

    private String schemaEvolutionV1Jar(String root, String exampleSubFolder) {
        return String.format(
                "%s/examples/maven/%s/schema-evolution-v1/target/schema-evolution-v1-1.0-SNAPSHOT.jar",
                root, exampleSubFolder);
    }

    private String schemaEvolutionV2Jar(String root, String exampleSubFolder) {
        return String.format(
                "%s/examples/maven/%s/schema-evolution-v2/target/schema-evolution-v2-1.0-SNAPSHOT.jar",
                root, exampleSubFolder);
    }

    /**
     * Production schema-evolution flow: a job running with
     * {@code com.example.EvolvingPOJO (id, name)} takes a savepoint, the team
     * deploys a release that adds a {@code ts} field, the table gets
     * {@code ALTER ADD COLUMN ts}, and the new job restarts from the
     * savepoint. The test runs the two pre-built example apps from
     * {@code examples/maven/flink-v2/schema-evolution-{v1,v2}} as separate
     * jobs against the same Flink cluster.
     *
     * <p>Both apps ship the same FQ class name {@code com.example.EvolvingPOJO}
     * with different field structures, which is the only configuration Flink's
     * {@code PojoSerializerSnapshot} schema migration supports.
     *
     * <p>Skipped when running in Scala mode — these examples are Java-only.
     */
    @Test
    void testSchemaEvolution() throws Exception {
        if (exampleLang == ExampleLang.SCALA) return;

        String root = getRoot();
        // Pick example variant matching the Flink version under test:
        // flink-v2 for 2.x/latest, flink-v1.7 for 1.x.
        String exampleSubFolder = exampleSubFolder(flinkVersion);
        String v1JarPath = schemaEvolutionV1Jar(root, exampleSubFolder);
        String v2JarPath = schemaEvolutionV2Jar(root, exampleSubFolder);

        File v1Jar = new File(v1JarPath);
        File v2Jar = new File(v2JarPath);
        if (!v1Jar.exists() || !v2Jar.exists()) {
            System.out.println("Skipping testSchemaEvolution — example JARs not built. "
                    + "Run mvn -q clean package in examples/maven/flink-v2/schema-evolution-{v1,v2}");
            return;
        }

        String tableName = "evolving";
        String clickHouseURL = ClickHouseServerForTests.getURLForCluster();
        String username = ClickHouseServerForTests.getUsername();
        String password = ClickHouseServerForTests.getPassword();
        String database = ClickHouseServerForTests.getDatabase();

        // 1. Create the V1-shape table.
        ClickHouseServerForTests.executeSql(
                String.format("DROP TABLE IF EXISTS `%s`.`%s`", database, tableName));
        ClickHouseServerForTests.executeSql(
                String.format("CREATE TABLE `%s`.`%s` (id Int32, name String) "
                        + "ENGINE = MergeTree ORDER BY id", database, tableName));

        // 2. Bring up Flink with a host-mounted shared state directory so savepoints
        //    survive across the two distinct JARs / jobs.
        Network network = ClickHouseServerForTests.getNetwork();
        Path sharedStateDir = Files.createTempDirectory("flink-schema-evo-state-");
        // Permissive perms so the in-container Flink user can write here.
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
            // 3. Upload + run V1.
            String v1JarId = cluster.uploadJar(v1JarPath);
            String v1JobId = cluster.runJob(v1JarId, "com.example.Main", 1,
                    "-url", clickHouseURL,
                    "-username", username,
                    "-password", password,
                    "-database", database,
                    "-table", tableName,
                    "-records", "100");

            // 4. Wait for source emission and at least one checkpoint to land. The
            //    sink uses maxTimeInBufferMS = 600_000 so nothing flushes meanwhile.
            Thread.sleep(8000);
            Assertions.assertEquals(0, ClickHouseServerForTests.countRows(tableName),
                    "V1 records should still be in writer buffer at savepoint time, not in ClickHouse");

            // 5. Take savepoint, then cancel V1.
            String savepointPath = cluster.triggerSavepoint(v1JobId, Cluster.CONTAINER_SAVEPOINT_DIR);
            Assertions.assertNotNull(savepointPath, "Savepoint trigger should return a location");
            cluster.cancelJob(v1JobId);

            // 6. Schema migration on the table itself.
            ClickHouseServerForTests.executeSql(String.format(
                    "ALTER TABLE `%s`.`%s` ADD COLUMN ts Int64 DEFAULT 0", database, tableName));

            // 7. Upload + run V2 from the V1 savepoint.
            String v2JarId = cluster.uploadJar(v2JarPath);
            String v2JobId = cluster.runJobFromSavepoint(v2JarId, "com.example.Main", 1, savepointPath,
                    "-url", clickHouseURL,
                    "-username", username,
                    "-password", password,
                    "-database", database,
                    "-table", tableName,
                    "-records", "100",
                    "-idStart", "100",
                    "-ts", "42000");
            Assertions.assertNotNull(v2JobId, "V2 job submission should succeed");

            // 8. Wait for restored V1 records (migrated to V2 with ts=0) and fresh V2
            //    records (ts=42000) to flush.
            int expectedTotal = 200;
            int rows = 0;
            for (int i = 0; i < 60; i++) {
                Thread.sleep(1000);
                rows = ClickHouseServerForTests.countRows(tableName);
                if (rows == expectedTotal) break;
            }

            cluster.cancelJob(v2JobId);

            Assertions.assertEquals(expectedTotal, rows,
                    "After schema-evolved restore, all V1 buffered records must arrive as V2 with "
                            + "ts=0 alongside the 100 freshly emitted V2 records.");

            int v1Origin = ClickHouseServerForTests.countRowsWhere(tableName, "ts = 0");
            int v2Origin = ClickHouseServerForTests.countRowsWhere(tableName, "ts = 42000");
            Assertions.assertEquals(100, v1Origin,
                    "Expected 100 rows from V1 origin (ts defaulted to 0 by schema migration)");
            Assertions.assertEquals(100, v2Origin,
                    "Expected 100 rows from fresh V2 emission (ts=42000)");
        } finally {
            cluster.tearDown();
        }
    }
}
