package com.migration.source.controller;


import com.migration.source.consumer.KafkaToMySQLMigration;
import com.migration.source.service.MongoService;
import com.migration.source.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class SchemaController {

    @Autowired
    private SchemaService schemaService;

     @Autowired
    private MongoService mongoService;

    @Autowired
    private KafkaToMySQLMigration migrationService;

    @GetMapping("/send/{topic}/{tableName}")
    public String sendDataToKafka(@PathVariable String topic, @PathVariable String tableName) {
        try {
            schemaService.sendDataToKafka(topic, tableName);
            return "Data sent to Kafka successfully for topic: " + topic;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to send data to Kafka for topic: " + topic;
        }
    }

    @GetMapping("/send-to-kafka/{topic}/{collectionName}")
    public ResponseEntity<String> sendDataToKafkaFromMongo(@PathVariable String topic, @PathVariable String collectionName) {
        try {
            mongoService.sendDataToKafka(topic, collectionName);
            return ResponseEntity.ok("Data sent to Kafka successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error sending data to Kafka: " + e.getMessage());
        }
    }

    @PostMapping("/migrate")
    public void migrateDataFromKafkaToMySQL(
            @RequestParam("kafkaTopic") String kafkaTopic,
            @RequestParam("tableName") String tableName) {
        migrationService.migrateDataFromKafkaToMySQL(kafkaTopic, tableName);
    }

    @GetMapping("/tables")
    public List<String> getAllTables() {
        try {
            return schemaService.getAllTables();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
