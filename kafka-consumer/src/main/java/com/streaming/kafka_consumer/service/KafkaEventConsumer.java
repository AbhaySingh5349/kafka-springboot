package com.streaming.kafka_consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Slf4j
@Component
public class KafkaEventConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "sb-topic-1", groupId = "streaming-group")
    public void consumer1(String msg){
        log.info("MESSAGE: " + msg);

        try {
//            ObjectNode objectNode = objectMapper.readValue(msg, ObjectNode.class);
//
//            String title = objectNode.get("title").textValue();
//            String description = objectNode.get("description").textValue();

            JsonNode jsonNode = objectMapper.readTree(msg);
            if (jsonNode.isTextual()) {
                jsonNode = objectMapper.readTree(jsonNode.asText());
            }

            String title = jsonNode.has("title") ? jsonNode.get("title").asText() : "Unknown Title";
            String description = jsonNode.has("description") ? jsonNode.get("description").asText() : "Unknown Description";

            log.info("Parsed Title: " + title);
            log.info("Parsed Description: " + description);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group")
    public void consumer2(String msg){
        log.info("consumer1 for sb-topic-2: " + msg);
    }

    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group")
    public void consumer3(String msg){
        log.info("consumer2 for sb-topic-2: " + msg);
    }

    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group")
    public void consumer4(String msg){
        log.info("consumer3 for sb-topic-2: " + msg);
    }
}
