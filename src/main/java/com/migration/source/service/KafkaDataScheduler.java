package com.migration.source.service;

import lombok.AllArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class KafkaDataScheduler {
    private final SchemaService schemaService;
/*
    @Scheduled(fixedDelay = 60000)
    public void sendDataToKafka() {
        try {
            String topic = "product-topic";
            String tableName = "product";
            schemaService.sendDataToKafka(topic, tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

 */
}
