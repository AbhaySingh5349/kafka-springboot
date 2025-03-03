package com.streaming.kafka_producer.service;

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

    public void sendMsgToTopic(Event event){
        String topic = event.getTopic();
        String msg = event.getMessage();

        CompletableFuture<SendResult<String, Object>> future = template.send(topic, msg);

        // callback
        future.whenComplete((res, ex) -> {
            if(ex == null){
                System.out.println("Sent message=[ " + msg + " ] with offset=[ " + res.getRecordMetadata().offset() + " ]");
            }else{
                System.out.println("Failed to send message=[" + msg + " ] due to: " + ex.getMessage());
            }
        });
    }
}
