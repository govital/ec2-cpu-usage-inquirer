package com.example.ec2_cpu_usages.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Service
public class ParquetFileProcessor {

    private static final String METRIC_FILE = "src/main/resources/metrics.parquet";
    private static final String RESOURCES_FILE = "src/main/resources/resources.parquet";
    private static final String OUTPUT_FILE = "src/main/resources/processed_data.json";

    private final Map<String, List<Map<String, Object>>> processedData = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        File file = new File(OUTPUT_FILE);
        if (file.exists()) {
            System.out.println("Loading preprocessed data...");
            loadProcessedData();
        } else {
            System.out.println("Processing Parquet files...");
            processAndSaveData();
        }
    }

    public void processAndSaveData() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "file:///");

            // Read Resources File (ARN Mapping)
            Map<String, String> arnMapping = new HashMap<>();
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(RESOURCES_FILE))
                    .withConf(configuration)
                    .build()) {

                Group record;
                while ((record = reader.read()) != null) {
                    String attributesJson = record.getString("attributes", 0);
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, String> attributes = mapper.readValue(attributesJson, new TypeReference<>() {});
                    if (attributes.containsKey("instance_id") && attributes.containsKey("arn")) {
                        arnMapping.put(attributes.get("instance_id"), attributes.get("arn"));
                    }
                }
            }

            // Read Metrics File and Aggregate Data by ARN
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(METRIC_FILE))
                    .withConf(configuration)
                    .build()) {

                Group record;
                while ((record = reader.read()) != null) {
                    String instanceId = extractInstanceId(record.getString("dimensions", 0));
                    if (!arnMapping.containsKey(instanceId)) continue;  // Skip if no ARN mapping exists

                    String arn = arnMapping.get(instanceId);
                    String timestamp = record.getString("timestamp", 0);
                    double value = record.getDouble("value", 0);

                    processedData.computeIfAbsent(arn, k -> new ArrayList<>()).add(Map.of(
                            "timestamp", timestamp,
                            "value", value
                    ));
                }
            }

            // Save Processed Data to File
            objectMapper.writeValue(new File(OUTPUT_FILE), processedData);
            System.out.println("Processed data saved successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadProcessedData() {
        try {
            byte[] jsonData = Files.readAllBytes(Paths.get(OUTPUT_FILE));
            Map<String, List<Map<String, Object>>> data = objectMapper.readValue(jsonData, new TypeReference<>() {});
            processedData.putAll(data);
            System.out.println("Preprocessed data loaded successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Map<String, Object>> getDataForArn(String arn) {
        return processedData.getOrDefault(arn, Collections.emptyList());
    }

    private String extractInstanceId(String jsonArn) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = objectMapper.readTree(jsonArn);
            return node.has("InstanceId") ? node.get("InstanceId").asText() : "";
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}
