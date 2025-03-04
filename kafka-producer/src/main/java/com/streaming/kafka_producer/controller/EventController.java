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

/*
{
  "topic": "sb-topic-1",
  "message": {
    "type": "string",
    "message": "Order shipped successfully!"
  }
}

{
  "topic": "sb-topic-1",
  "message": {
    "type": "customer",
    "customer": {
      "id": 101,
      "name": "John Doe",
      "email": "john.doe@example.com"
    }
  }
}
*/

@Controller
@RequestMapping("/kafka/producer")
public class EventController {
    @Autowired
    private KafkaEventPublisher publisher;

    @PostMapping("/events")
    public ResponseEntity<?> publishMsg(@RequestBody Event event){
        try {
//            System.out.println("Event: " + event.toString());
            publisher.sendMsgToTopic(event);
            return ResponseEntity.ok("message published successfully !");
        }catch (Exception e){
            System.out.println("Producer ERROR: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
