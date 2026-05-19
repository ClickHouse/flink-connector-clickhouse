package com.clickhouse.flink;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Cluster {

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final int INTERNAL_REST_PORT = 8081;
    private static final int INTERNAL_JOB_MANAGER_RCP_PORT = 6123;

    private GenericContainer<?> containerJobManager;
    private List<GenericContainer<?>> containerTaskManagerList = new ArrayList<>();

    /** Path inside Flink containers where shared state (checkpoints/savepoints) lives. */
    public static final String CONTAINER_STATE_DIR = "/tmp/flink-state";
    /** Subdirectory of {@link #CONTAINER_STATE_DIR} used for savepoints. */
    public static final String CONTAINER_SAVEPOINT_DIR = CONTAINER_STATE_DIR + "/savepoints";

    public static class Builder {
        private String flinkVersion;
        private int taskManagers;
        private String sourcePath;
        private String dataFilename;
        private String targetPath;
        private Network network;
        private String sharedStateHostPath;

        public Builder() {
            taskManagers = 1;
            flinkVersion = "latest";
            sourcePath = null;
            dataFilename = null;
            targetPath = null;
            network = null;
            sharedStateHostPath = null;
        }
        public Builder withTaskManagers(int taskManagers) {
            this.taskManagers = taskManagers;
            return this;
        }

        public Builder withFlinkVersion(String flinkVersion) {
            this.flinkVersion = flinkVersion;
            return this;
        }

        public Builder withDataFile(String sourcePath, String dataFilename, String targetPath) {
            this.sourcePath = sourcePath;
            this.dataFilename = dataFilename;
            this.targetPath = targetPath;
            return this;
        }

        public Builder withNetwork(Network network) {
            this.network = network;
            return this;
        }

        /**
         * Mounts a host directory into every Flink container at
         * {@link #CONTAINER_STATE_DIR}, and configures the cluster with
         * {@code state.checkpoints.dir} / {@code state.savepoints.dir} so that
         * savepoints written by the JobManager are visible to the TaskManagers
         * (and survive a job restart from a different JAR).
         */
        public Builder withSharedStateVolume(String hostPath) {
            this.sharedStateHostPath = hostPath;
            return this;
        }

        public Cluster build() {
            // when we are not specifying a network we should create one
            if (network == null) {
                network = Network.newNetwork();
            }
            Cluster cluster = new Cluster(flinkVersion, taskManagers, sourcePath, dataFilename, targetPath, network, sharedStateHostPath);
            return cluster;
        }

    }

    public Cluster(String flinkVersion, int taskManagers, String sourcePath, String dataFilename, String targetPath, Network network) {
        this(flinkVersion, taskManagers, sourcePath, dataFilename, targetPath, network, null);
    }

    public Cluster(String flinkVersion, int taskManagers, String sourcePath, String dataFilename, String targetPath, Network network, String sharedStateHostPath) {
        boolean hasDataFile = sourcePath != null;
        MountableFile mountableFile = hasDataFile ? MountableFile.forHostPath(sourcePath + dataFilename) : null;
        String dataFileInContainer = hasDataFile ? String.format("%s/%s", targetPath, dataFilename) : null;
        String flinkImageTag = String.format("flink:%s", flinkVersion);
        LOG.info("Using flink image tag: {}", flinkImageTag);
        if (hasDataFile) {
            LOG.info("Data file location in container: {}", dataFileInContainer);
        }
        DockerImageName FLINK_IMAGE = DockerImageName.parse(flinkImageTag);

        StringBuilder flinkProps = new StringBuilder("jobmanager.rpc.address: jobmanager");
        if (sharedStateHostPath != null) {
            flinkProps.append("\nstate.checkpoints.dir: file://").append(CONTAINER_STATE_DIR).append("/checkpoints");
            flinkProps.append("\nstate.savepoints.dir: file://").append(CONTAINER_SAVEPOINT_DIR);
        }
        String flinkPropsValue = flinkProps.toString();

        containerJobManager = new GenericContainer<>(FLINK_IMAGE)
                .withCommand("jobmanager")
                .withNetwork(network)
                .withExposedPorts(INTERNAL_REST_PORT, INTERNAL_JOB_MANAGER_RCP_PORT)
                .withNetworkAliases("jobmanager")
                .withEnv("FLINK_PROPERTIES", flinkPropsValue);

        if (hasDataFile) {
            containerJobManager.withCopyFileToContainer(mountableFile, dataFileInContainer);
        }
        if (sharedStateHostPath != null) {
            containerJobManager.withFileSystemBind(sharedStateHostPath, CONTAINER_STATE_DIR);
        }
        for (int i = 0; i < taskManagers; i++) {
            GenericContainer<?> containerTaskManager = new GenericContainer<>(FLINK_IMAGE)
                    .withCommand("taskmanager")
                    .withNetwork(network)
                    .dependsOn(containerJobManager)
                    .withEnv("FLINK_PROPERTIES", flinkPropsValue);
            if (hasDataFile) {
                containerTaskManager.withCopyFileToContainer(mountableFile, dataFileInContainer);
            }
            if (sharedStateHostPath != null) {
                containerTaskManager.withFileSystemBind(sharedStateHostPath, CONTAINER_STATE_DIR);
            }
            containerTaskManagerList.add(containerTaskManager);
        }

        LOG.info("Starting JobManager");
        containerJobManager.start();
        LOG.info("Using task managers: {} and starting taskManagers", containerTaskManagerList.size());
        for (int i = 0; i < taskManagers; i++) {
            containerTaskManagerList.get(i).start();
        }
        // TODO: add strategy for wait
    }

    public int getDashboardPort() {
        return containerJobManager.getMappedPort(INTERNAL_REST_PORT);
    }

    public String getDashboardUrl() {
        return String.format("%s:%s",containerJobManager.getContainerIpAddress(), getDashboardPort());
    }

    public GenericContainer<?> getContainerJobManager() {
        return containerJobManager;
    }

    public List<GenericContainer<?>> getContainerTaskManagerList() {
        return containerTaskManagerList;
    }

    public String uploadJar(String jarFilePath) throws IOException {
        File jarFile = new File(jarFilePath);
        String clusterURLJarUploadAPI = String.format("http://%s/jars/upload", getDashboardUrl());
        OkHttpClient client = new OkHttpClient();
        RequestBody fileBody = RequestBody.create(jarFile, MediaType.parse("application/java-archive"));
        MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("jarfile", jarFile.getName(), fileBody)
                .build();
        Request request = new Request.Builder()
                .url(clusterURLJarUploadAPI)
                .post(requestBody).build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("uploadJar failed code: {}", response.code());
            return null;
        } else {
            Gson gson = new Gson();
            String responseBody = response.body().string();
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("status") &&
                jsonObject.get("status").getAsString().equalsIgnoreCase("success") &&
                jsonObject.has("filename") ) {
                String filename = jsonObject.get("filename").getAsString();
                // Flink returns an absolute path under its upload dir; the jar id
                // used by /jars/{id}/run is just the basename (uuid_<originalname>.jar).
                int slash = filename.lastIndexOf('/');
                String jarId = (slash >= 0) ? filename.substring(slash + 1) : filename;
                LOG.info("Uploaded jar id: {}", jarId);
                return jarId;
            }
            return null;
        }


    }
    public List<String> listAllJars() throws IOException {
        String clusterURLListJars = String.format("http://%s/jars", getDashboardUrl());

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(clusterURLListJars)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("listAllJars failed code: {}", response.code());
            return null;
        } else {
            Gson gson = new Gson();
            String responseBody = response.body().string();
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            List<String> jars = new ArrayList<>();
            if (jsonObject.has("files")) {
                JsonArray jsonArray = jsonObject.getAsJsonArray("files");
                for (JsonElement element : jsonArray) {
                    if (element.getAsJsonObject().has("id")) {
                        jars.add(element.getAsJsonObject().get("id").getAsString());
                        LOG.info("listAllJars successful: {}", element.getAsJsonObject().get("id").getAsString());
                    }
            }

            }
            return jars;
        }


    }

    public String runJob(String jarId, String entryClass, int parallelism, String... args) throws IOException {
        String programArg = String.join(",", args);
        String clusterURLJarRunAPI = String.format("http://%s/jars/%s/run?programArg=%s", getDashboardUrl(), jarId, programArg);
        RequestBody body = RequestBody.create(
                "",
                MediaType.get("application/json; charset=utf-8")
        );

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(clusterURLJarRunAPI)
                .post(body)
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("runJob failed code: {}", response.code());
            return null;
        } else {
            Gson gson = new Gson();
            String responseBody = response.body().string();
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("jobid") ) {
                String jobid = jsonObject.get("jobid").getAsString();
                LOG.info("runJob successful jobid: {}", jobid);
                return jobid;
            }
            return null;
        }
    }

    public String jobStatus(String jobId) throws IOException {
        String clusterURLJobStatusAPI = String.format("http://%s/jobs/%s", getDashboardUrl(), jobId);
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(clusterURLJobStatusAPI)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("jobStatus response code: {}", response.code());
            return null;
        } else {
            Gson gson = new Gson();
            String responseBody = response.body().string();
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("state")) {
                String state = jsonObject.get("state").getAsString();
                LOG.info("jobStatus state: {}", state);
                return state;
            }
            return null;
        }
    }

    /**
     * Triggers a savepoint, polls until completion, returns the savepoint
     * location reported by the JobManager. The location is a path inside the
     * Flink containers (under {@link #CONTAINER_SAVEPOINT_DIR} when the cluster
     * was built with a shared state volume).
     *
     * @param jobId           the running job's id
     * @param targetDirectory in-container path where the savepoint should be written
     * @return the savepoint location reported by Flink (suitable for {@link #runJobFromSavepoint})
     */
    public String triggerSavepoint(String jobId, String targetDirectory) throws IOException, InterruptedException {
        String url = String.format("http://%s/jobs/%s/savepoints", getDashboardUrl(), jobId);
        JsonObject body = new JsonObject();
        body.addProperty("target-directory", targetDirectory);
        body.addProperty("cancel-job", false);

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(body.toString(), MediaType.get("application/json; charset=utf-8")))
                .build();
        Response response = client.newCall(request).execute();
        String responseBody = response.body() != null ? response.body().string() : "";
        if (!response.isSuccessful()) {
            throw new IOException("triggerSavepoint failed: HTTP " + response.code() + " body=" + responseBody);
        }
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(responseBody, JsonObject.class);
        if (!json.has("request-id")) {
            throw new IOException("triggerSavepoint response missing request-id: " + responseBody);
        }
        String triggerId = json.get("request-id").getAsString();
        System.out.println("Savepoint triggered, request-id: " + triggerId);
        return waitForSavepointCompletion(jobId, triggerId);
    }

    private String waitForSavepointCompletion(String jobId, String triggerId) throws IOException, InterruptedException {
        String url = String.format("http://%s/jobs/%s/savepoints/%s", getDashboardUrl(), jobId, triggerId);
        OkHttpClient client = new OkHttpClient();
        String lastBody = "";
        for (int i = 0; i < 120; i++) {
            Request request = new Request.Builder().url(url).build();
            Response response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                lastBody = response.body() != null ? response.body().string() : "";
                Gson gson = new Gson();
                JsonObject json = gson.fromJson(lastBody, JsonObject.class);
                if (json.has("status")) {
                    String statusId = json.getAsJsonObject("status").get("id").getAsString();
                    if ("COMPLETED".equalsIgnoreCase(statusId)) {
                        if (json.has("operation")) {
                            JsonObject op = json.getAsJsonObject("operation");
                            if (op.has("location") && !op.get("location").isJsonNull()) {
                                String location = op.get("location").getAsString();
                                System.out.println("Savepoint completed at: " + location);
                                return location;
                            }
                            if (op.has("failure-cause")) {
                                throw new IOException("Savepoint failed: " + op.get("failure-cause"));
                            }
                            throw new IOException("Savepoint COMPLETED but operation has no location and no failure-cause: " + lastBody);
                        }
                    }
                }
            }
            Thread.sleep(500);
        }
        throw new IOException("Savepoint did not complete within timeout. Last body: " + lastBody);
    }

    /**
     * Cancels a running job. Returns true on a 2xx response from the REST API.
     */
    public boolean cancelJob(String jobId) throws IOException {
        String url = String.format("http://%s/jobs/%s?mode=cancel", getDashboardUrl(), jobId);
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .patch(RequestBody.create("", MediaType.get("application/json; charset=utf-8")))
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("cancelJob failed code: {}", response.code());
            return false;
        }
        LOG.info("cancelJob successful for jobId: {}", jobId);
        return true;
    }

    /**
     * Submits a job from a previously-uploaded JAR, restoring writer state from
     * the given savepoint path (in-container). Mirrors {@link #runJob} but POSTs
     * a JSON body so the {@code savepointPath} can be set.
     */
    public String runJobFromSavepoint(String jarId, String entryClass, int parallelism, String savepointPath, String... args) throws IOException {
        String url = String.format("http://%s/jars/%s/run", getDashboardUrl(), jarId);
        JsonObject body = new JsonObject();
        body.addProperty("entryClass", entryClass);
        body.addProperty("parallelism", parallelism);
        body.addProperty("savepointPath", savepointPath);
        body.addProperty("allowNonRestoredState", false);
        if (args != null && args.length > 0) {
            JsonArray arr = new JsonArray();
            for (String a : args) {
                arr.add(a);
            }
            body.add("programArgsList", arr);
        }

        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(body.toString(), MediaType.get("application/json; charset=utf-8")))
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            LOG.error("runJobFromSavepoint failed code: {} body: {}", response.code(),
                    response.body() != null ? response.body().string() : "");
            return null;
        }
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(response.body().string(), JsonObject.class);
        if (json.has("jobid")) {
            String jobid = json.get("jobid").getAsString();
            LOG.info("runJobFromSavepoint successful jobid: {}", jobid);
            return jobid;
        }
        return null;
    }

    public void tearDown() {
        if (containerTaskManagerList != null) {
            containerTaskManagerList.forEach(GenericContainer::stop);
        }
        if (containerJobManager != null) {
            containerJobManager.stop();
        }
    }

}
