package org.apache.flink.connector.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class EmbeddedFlinkClusterForTests {

    protected static MiniClusterWithClientResource flinkCluster = null;
    @BeforeAll
    static void setUp() throws Exception {
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081); // web UI port (optional)
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(3)
                        .setConfiguration(config)
                        .build());
        flinkCluster.before();
    }
    @AfterAll
    static void tearDown() {
        if (flinkCluster != null) {
            flinkCluster.after();
        }
    }

    protected static MiniClusterWithClientResource getMiniCluster() {
        if (flinkCluster == null)
            throw new RuntimeException("No MiniCluster available");
        return flinkCluster;
    }
}
