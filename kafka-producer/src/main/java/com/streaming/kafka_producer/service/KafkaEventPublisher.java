package com.streaming.kafka_producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streaming.kafka_producer.dto.Event;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaEventPublisher {
    @Autowired
    private KafkaTemplate<String, Object> template;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendMsgToTopic(Event event){
        String topic = event.getTopic();
        String title = event.getTitle();
        String description = event.getDescription();

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("title", title);
        objectNode.put("description", description);

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
    }
}
