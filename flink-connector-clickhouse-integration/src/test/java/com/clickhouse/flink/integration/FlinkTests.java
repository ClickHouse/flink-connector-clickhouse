package com.clickhouse.flink.integration;

import com.clickhouse.flink.Cluster;
import org.apache.flink.connector.test.embedded.clickhouse.ClickHouseServerForTests;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FlinkTests {

            // "/Users/mzitnik/clickhouse/dev/integrations/flink-connector-clickhouse/examples/maven/covid/target/covid-1.0-SNAPSHOT.jar";
    static String flinkVersion = "latest";
    @BeforeAll
    public static void setUp() throws Exception {
        flinkVersion = (System.getenv("FLINK_VERSION") != null ? System.getenv("FLINK_VERSION") : "latest");
        System.out.println("FLINK_VERSION: " + flinkVersion);
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
            new RuntimeException("Can not get root path");
        return null;
    }

    private String exampleSubFolder(String flinkVersion) {
        if (flinkVersion.equalsIgnoreCase("latest") || flinkVersion.startsWith("2.0"))
            return "flink-v2";
        return "flink-v1.7";
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
        String jarLocation = String.format("%s/examples/maven/%s/covid/target/covid-1.0-SNAPSHOT.jar", root, exampleSubFolder);
        String dataFile = "100k_epidemiology.csv";
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
        System.out.println(cluster.getDashboardPort());
        System.out.println(cluster.getDashboardUrl());
        String jarId = cluster.uploadJar(jarLocation);
        System.out.println(jarId);
        List<String> jars = cluster.listAllJars();
        String jobId = cluster.runJob(jars.get(0),
                "com.clickhouse.example.covid.DataStreamJob",
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
            System.out.println(state);
            int count = ClickHouseServerForTests.countRows(tableName);
            if (count ==  100000)
                break;
            System.out.println(count);
            Thread.sleep(2000);
        }
        int count = ClickHouseServerForTests.countRows(tableName);
        System.out.println(count);
        Assert.assertEquals(100000, count);
    }
}
