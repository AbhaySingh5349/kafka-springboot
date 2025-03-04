package com.streaming.kafka_producer.dto;

import lombok.*;

@Data
@AllArgsConstructor
public class Event {
    private String topic;
    private String title;
    private String description;
}
