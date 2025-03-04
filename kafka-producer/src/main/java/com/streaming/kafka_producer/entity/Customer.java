package com.streaming.kafka_producer.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Customer {
    private int id;
    private String name;
    private String email;
}
