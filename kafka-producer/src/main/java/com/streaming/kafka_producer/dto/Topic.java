package com.streaming.kafka_producer.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Topic {
    private String topicName;
    private int partitions;
    private short replicationFactor;
}
