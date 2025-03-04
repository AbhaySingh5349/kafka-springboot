package com.streaming.kafka_producer.dto;

import lombok.*;

@Data
@AllArgsConstructor
public class Topic {
    private String topicName;
    private int partitions;
    private short replicationFactor;
}
