package com.streaming.kafka_consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Component
public class KafkaEventConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "sb-topic-1", groupId = "streaming-group")
    public void consumer1(String msg){
        log.info("MESSAGE: " + msg);

        try {
            ObjectNode objectNode = objectMapper.readValue(msg, ObjectNode.class);

            String title = objectNode.get("title").textValue();
            String description = objectNode.get("description").textValue();

            log.info("Parsed Title: " + title);
            log.info("Parsed Description: " + description);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    // default attempt time is 3
    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = "sb-topic-err-handle", groupId = "streaming-group")
    public void consumerWithErrorHandling(String msg){
        log.info("MESSAGE: " + msg);

        List<String> restrictedIp = Stream.of("32.241.244.236", "15.55.49.164").toList();

        try {
            ObjectNode objectNode = objectMapper.readValue(msg, ObjectNode.class);

            String title = objectNode.get("title").textValue();
            String description = objectNode.get("description").textValue();
            String ipAddress = objectNode.get("ipAddress").textValue();

            if(restrictedIp.contains(ipAddress)){
                throw new RuntimeException("ERROR Invalid IP Address: " + ipAddress);
            }

            log.info("Parsed Title: " + title);
            log.info("Parsed Description: " + description);
            log.info("Parsed IP: " + ipAddress);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
     */

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 2000))
    @KafkaListener(topics = "sb-topic-err-handle", groupId = "streaming-group")
    public void consumeWithErrorHandling(
            @Payload String msg,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = "content-type", required = false) String contentType,
            @Header(name = "event-source", required = false) String eventSource
    ) {
        log.info("📩 MESSAGE RECEIVED: {}", msg);
        log.info("📌 Topic: {}, Content-Type: {}, Event Source: {}", topic, contentType, eventSource);

        List<String> restrictedIps = List.of("32.241.244.236", "15.55.49.164");

        try {
            JsonNode objectNode = objectMapper.readTree(msg);
            String title = objectNode.get("title").textValue();
            String description = objectNode.get("description").textValue();
            String ipAddress = objectNode.get("ipAddress").textValue();

            if (restrictedIps.contains(ipAddress)) {
                throw new RuntimeException("❌ ERROR: Restricted IP Address - " + ipAddress);
            }

            log.info("✅ Parsed: Title: {}, Description: {}, IP: {}", title, description, ipAddress);
        } catch (DeserializationException e) {
            log.error("⚠️ Invalid JSON format: {}", msg);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("❌ Error processing message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // ✅ DLT Handler – When retries fail, message goes here
    @DltHandler
    public void consumeDeadLetterQueueMessage(String msg) {
        log.error("🔥 DLT MESSAGE RECEIVED: " + msg);
    }

    // we can also define to which particular partition it needs to read from
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

    /*
    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group",
            topicPartitions = {@TopicPartition(topic = "sb-topic-2", partitions = {"0"})})
    public void consumer2(String msg){
        log.info("consumer1 for sb-topic-2: " + msg);
    }

    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group",
            topicPartitions = {@TopicPartition(topic = "sb-topic-2", partitions = {"1"})})
    public void consumer3(String msg){
        log.info("consumer2 for sb-topic-2: " + msg);
    }

    @KafkaListener(topics = "sb-topic-2", groupId = "streaming-group",
            topicPartitions = {@TopicPartition(topic = "sb-topic-2", partitions = {"2"})})
    public void consumer4(String msg){
        log.info("consumer3 for sb-topic-2: " + msg);
    }
     */
}
