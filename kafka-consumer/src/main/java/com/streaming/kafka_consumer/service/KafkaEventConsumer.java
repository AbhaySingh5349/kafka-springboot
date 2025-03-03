package com.streaming.kafka_consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaEventConsumer {

    @KafkaListener(topics = "sb-topic-1", groupId = "streaming-group")
    public void consumer1(String msg){
        log.info("consumer for sb-topic-1: " + msg);
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
