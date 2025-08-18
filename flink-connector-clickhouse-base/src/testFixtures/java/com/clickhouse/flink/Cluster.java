package com.clickhouse.flink;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.*;
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

    private static final int INTERNAL_REST_PORT = 8081;
    private static final int INTERNAL_JOB_MANAGER_RCP_PORT = 6123;

    private GenericContainer<?> containerJobManager;
    private List<GenericContainer<?>> containerTaskManagerList = new ArrayList<>();

    public static class Builder {
        private String flinkVersion;
        private int taskManagers;
        private String sourcePath;
        private String dataFilename;
        private String targetPath;
        private Network network;

        public Builder() {
            taskManagers = 1;
            flinkVersion = "latest";
            sourcePath = null;
            dataFilename = null;
            targetPath = null;
            network = null;
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

        public Cluster build() {
            // when we are not specifying a network we should create one
            if (network == null) {
                network = Network.newNetwork();
            }
            Cluster cluster = new Cluster(flinkVersion, taskManagers, sourcePath, dataFilename, targetPath, network);
            return cluster;
        }

    }

    public Cluster(String flinkVersion, int taskManagers, String sourcePath, String dataFilename, String targetPath, Network network) {
        MountableFile mountableFile = MountableFile.forHostPath(sourcePath + dataFilename);
        String dataFileInContainer = String.format("%s/%s", targetPath, dataFilename);
        String flinkImageTag = String.format("flink:%s", flinkVersion);
        DockerImageName FLINK_IMAGE = DockerImageName.parse(flinkImageTag);
        containerJobManager = new GenericContainer<>(FLINK_IMAGE)
                .withCommand("jobmanager")
                .withNetwork(network)
                .withExposedPorts(INTERNAL_REST_PORT, INTERNAL_JOB_MANAGER_RCP_PORT)
                .withNetworkAliases("jobmanager")
                .withEnv("FLINK_PROPERTIES","jobmanager.rpc.address: jobmanager");

        if (sourcePath != null) {
            containerJobManager.withCopyFileToContainer(mountableFile, dataFileInContainer);
        }
        for (int i = 0; i < taskManagers; i++) {
            GenericContainer<?> containerTaskManager = new GenericContainer<>(FLINK_IMAGE)
                    .withCommand("taskmanager")
                    .withNetwork(network)
                    .dependsOn(containerJobManager)
                    .withEnv("FLINK_PROPERTIES","jobmanager.rpc.address: jobmanager");
            if (sourcePath != null) {
                containerTaskManager.withCopyFileToContainer(mountableFile, dataFileInContainer);
            }
            containerTaskManagerList.add(containerTaskManager);
        }

        containerJobManager.start();
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
            System.err.println("Unexpected code " + response);
            return null;
        } else {
            Gson gson = new Gson();
            System.out.println("Upload successful!");
            String responseBody = response.body().string();
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("status") &
                jsonObject.get("status").getAsString().equalsIgnoreCase("success") &
                jsonObject.has("filename") ) {
                String filename = jsonObject.get("filename").getAsString();
                System.out.println("filename: " + filename);
                return filename;
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
            System.err.println("Unexpected code " + response);
            return null;
        } else {
            Gson gson = new Gson();
            String responseBody = response.body().string();
            System.out.println(responseBody);
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            List<String> jars = new ArrayList<>();
            if (jsonObject.has("files")) {
                JsonArray jsonArray = jsonObject.getAsJsonArray("files");
                for (JsonElement element : jsonArray) {
                    if (element.getAsJsonObject().has("id")) {
                        jars.add(element.getAsJsonObject().get("id").getAsString());
                        System.out.println("id: " + element.getAsJsonObject().get("id").getAsString());
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
            System.err.println("Unexpected code " + response);
            return null;
        } else {
            Gson gson = new Gson();
            System.out.println("Upload successful!");
            String responseBody = response.body().string();
            System.out.println(responseBody);
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("jobid") ) {
                String jobid = jsonObject.get("jobid").getAsString();
                System.out.println("jobid: " + jobid);
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
            System.err.println("Unexpected code " + response);
            return null;
        } else {
            Gson gson = new Gson();
            System.out.println("Upload successful!");
            String responseBody = response.body().string();
            System.out.println(responseBody);
            JsonObject jsonObject = gson.fromJson(responseBody, JsonObject.class);
            if (jsonObject.has("state")) {
                String state = jsonObject.get("state").getAsString();
                System.out.println("state: " + state);
                return state;
            }
            return null;
        }
    }

    public void tearDown() {

    }

}
