package com.streaming.kafka_producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streaming.kafka_producer.dto.Event;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaEventPublisher {
    @Autowired
//    private KafkaTemplate<String, Object> template;
    private KafkaTemplate<String, String> template;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /*
    public void sendMsgToTopic(Event event){
        String topic = event.getTopic();
        String title = event.getTitle();
        String description = event.getDescription();
        String ipAddress = event.getIpAddress();

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("title", title);
        objectNode.put("description", description);
        objectNode.put("ipAddress", ipAddress);

        String kafkaMsg = objectNode.toString();

        CompletableFuture<SendResult<String, Object>> future = template.send(topic, kafkaMsg);

        // callback
        future.whenComplete((res, ex) -> {
            if(ex == null){
                System.out.println("Sent message=[ " + kafkaMsg + " ] with offset=[ " + res.getRecordMetadata().offset() + " ]");
            }else{
                System.out.println("Failed to send message=[" + kafkaMsg + " ] due to: " + ex.getMessage());
            }
        });

        // "send" is an overloaded method which can be used to have different configs (eg. send data to particular partition)
//        template.send(topic, 0, null, kafkaMsg + " part_0");
//        template.send(topic, 1, null, kafkaMsg + " part_1");
//        template.send(topic, 2, null, kafkaMsg + " part_2");
    }
     */

    public void sendMsgToTopicViaRecord(Event event){
        String topic = event.getTopic();
        String title = event.getTitle();
        String description = event.getDescription();
        String ipAddress = event.getIpAddress();

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("title", title);
        objectNode.put("description", description);
        objectNode.put("ipAddress", ipAddress);

        String kafkaMsg = objectNode.toString();

        // ✅ Send with headers
        Message<String> message = MessageBuilder
                .withPayload(kafkaMsg)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader("content-type", "application/json")  // 🔥 Important
                .setHeader("event-source", "backend-service")  // Custom Header
                .build();

        CompletableFuture<SendResult<String, String>> future = template.send(message);

        future.whenComplete((res, ex) -> {
            if (ex == null) {
                System.out.println("✅ Sent message=[" + kafkaMsg + "] with offset=[" + res.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("❌ Failed to send message=[" + kafkaMsg + "] due to: " + ex.getMessage());
            }
        });
    }
}
