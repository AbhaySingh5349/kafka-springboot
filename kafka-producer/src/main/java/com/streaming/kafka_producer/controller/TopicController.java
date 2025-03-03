package com.streaming.kafka_producer.controller;

import com.streaming.kafka_producer.dto.Topic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@Controller
@RequestMapping("/kafka/topic")
public class TopicController {
    @Autowired
    private KafkaAdmin kafkaAdmin;

    @PostMapping("")
    public ResponseEntity<String> createTopic(@RequestBody Topic request) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic newTopic = new NewTopic(request.getTopicName(), request.getPartitions(), request.getReplicationFactor());
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            return ResponseEntity.internalServerError().body("Error creating topic: " + e.getMessage());
        }

        return ResponseEntity.ok("Topic '" + request.getTopicName() + "' created successfully");
    }
}
