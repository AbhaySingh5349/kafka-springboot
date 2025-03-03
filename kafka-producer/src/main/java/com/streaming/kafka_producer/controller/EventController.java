package com.streaming.kafka_producer.controller;

import com.streaming.kafka_producer.dto.Event;
import com.streaming.kafka_producer.service.KafkaEventPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/kafka/producer")
public class EventController {
    @Autowired
    private KafkaEventPublisher publisher;

    @PostMapping("/events")
    public ResponseEntity<?> publishMsg(@RequestBody Event event){
        try {
            publisher.sendMsgToTopic(event);
            return ResponseEntity.ok("message published successfully !");
        }catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
