package com.example.ec2_cpu_usages;

import com.example.ec2_cpu_usages.service.ParquetFileProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetFileProcessorRealDataTest {

	private static final String PROCESSED_DATA_FILE = "src/main/resources/processed_data.json";
	private static final String TEST_ARN = "arn:aws:ec2:eu-central-1:488052111706:instance/i-0e34ad753d2ea802c";

	private ParquetFileProcessor parquetFileProcessor;

	@BeforeEach
	void setUp() {
		parquetFileProcessor = new ParquetFileProcessor();

		// If processed data file does not exist, create it using real processing logic
		File processedFile = new File(PROCESSED_DATA_FILE);
		if (!processedFile.exists()) {
			parquetFileProcessor.processAndSaveData();
		}
	}

	@Test
	void testArnExistsInProcessedData() throws Exception {
		File processedFile = new File(PROCESSED_DATA_FILE);
		assertTrue(processedFile.exists(), "Processed data file should exist.");

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(processedFile);

		assertTrue(rootNode.has(TEST_ARN), "Processed data should contain the test ARN.");

		JsonNode arnData = rootNode.get(TEST_ARN);
		assertNotNull(arnData, "Data for ARN should not be null.");
		assertTrue(arnData.isArray(), "ARN data should be an array.");
		assertFalse(arnData.isEmpty(), "ARN data should not be empty.");

		JsonNode firstEntry = arnData.get(0);
		assertNotNull(firstEntry.get("timestamp"));
		assertNotNull(firstEntry.get("value"));

		System.out.println("✔ ARN exists with valid data: " + firstEntry.toString());
	}
}

