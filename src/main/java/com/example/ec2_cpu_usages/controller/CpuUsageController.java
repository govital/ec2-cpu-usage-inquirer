package com.example.ec2_cpu_usages.controller;

import com.example.ec2_cpu_usages.service.ParquetFileProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1")
public class CpuUsageController {

    @Autowired
    private ParquetFileProcessor parquetFileProcessor;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @PostMapping("/refresh-processed-data")
    public String refreshProcessedData() {
        try {
            parquetFileProcessor.processAndSaveData(); // Force reprocess
            return "Processed data file successfully recreated!";
        } catch (Exception e) {
            e.printStackTrace();
            return "Error while regenerating processed_data.json: " + e.getMessage();
        }
    }

    @GetMapping("/cpu-usage")
    public double getAverageCpuUsage(
            @RequestParam String arn,
            @RequestParam Optional<String> start,
            @RequestParam Optional<String> end
    ) {
        try {

            // Get preprocessed data from HashMap
            List<Map<String, Object>> records = parquetFileProcessor.getDataForArn(arn);
            if (records == null || records.isEmpty()) {
                return 0;
            }

            double totalCpuUsage = 0;
            int count = 0;

            for (Map<String, Object> record : records) {
                String timestampStr = (String) record.get("timestamp");
                LocalDateTime timestamp = LocalDateTime.parse(timestampStr, formatter);
                double cpuUsage = (double) record.get("value");

                if ((!start.isPresent() || timestamp.isAfter(LocalDateTime.parse(start.get(), formatter))) &&
                        (!end.isPresent() || timestamp.isBefore(LocalDateTime.parse(end.get(), formatter)))) {
                    totalCpuUsage += cpuUsage;
                    count++;
                }
            }
            return count > 0 ? totalCpuUsage / count : 0;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error while regenerating processed_data.json: " + e.getMessage() );
            return 0;
        }
    }
}
