package org.apache.flink.connector.test.embedded.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

public class EmbeddedFlinkClusterForTests {

    static MiniClusterWithClientResource flinkCluster = null;
    static int REST_PORT = getFromEnvOrDefault("REST_PORT", 9091);
    static int NUM_TASK_SLOTS = getFromEnvOrDefault("NUM_TASK_SLOTS", 2);
    static int NUM_TASK_SLOTS_PER_TASK = getFromEnvOrDefault("NUM_TASK_SLOTS_PER_TASK" , 2);
    static int NUM_TASK_MANAGERS = getFromEnvOrDefault("NUM_TASK_MANAGERS",3);

    static int getFromEnvOrDefault(String key, int defaultValue) {
        String value = System.getenv().getOrDefault(key, String.valueOf(defaultValue));
        return Integer.parseInt(value);
    }

    public static void setUp() throws Exception {
        Configuration config = new Configuration();
        setUp(config);
    }

    public static void setUp(Configuration config) throws Exception {
        config.set(RestOptions.PORT, REST_PORT); // web UI port (optional)
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, NUM_TASK_SLOTS);
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS_PER_TASK)
                        .setNumberTaskManagers(NUM_TASK_MANAGERS)
                        .setConfiguration(config)
                        .build());
        flinkCluster.before();
    }

    public static void tearDown() {
        if (flinkCluster != null) {
            flinkCluster.after();
        }
    }

    public static MiniClusterWithClientResource getMiniCluster() {
        if (flinkCluster == null)
            throw new RuntimeException("No MiniCluster available");
        return flinkCluster;
    }
}
