package com.streaming.kafka_producer.controller;

import com.streaming.kafka_producer.dto.Message;
import com.streaming.kafka_producer.service.KafkaMsgPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/producer-app")
public class EventController {
    @Autowired
    private KafkaMsgPublisher publisher;

    @PostMapping("/messages")
    public ResponseEntity<?> publishMsg(@RequestBody Message msg){
        try {
            publisher.sendMsgToTopic(msg);
            return ResponseEntity.ok("message published successfully !");
        }catch (Exception e){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
