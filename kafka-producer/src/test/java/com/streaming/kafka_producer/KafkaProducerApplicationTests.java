package com.streaming.kafka_producer;

import com.streaming.kafka_producer.dto.Event;
import com.streaming.kafka_producer.service.KafkaEventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@Testcontainers
@SpringBootTest
class KafkaProducerApplicationTests {
//    @Container
//    static KafkaContainer kafkaContainer = new KafkaContainer(
//            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
//                    .asCompatibleSubstituteFor("apache/kafka")
//    );
//
//    @DynamicPropertySource
//    public static void initKafkaProperties(DynamicPropertyRegistry registry){
//        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
//    }
//
//    @Autowired
//    private KafkaEventPublisher kafkaEventPublisher;
//
//    @Test
//    public void testSendMsgToTopic(){
//        kafkaEventPublisher.sendMsgToTopic(new Event("test-topic-1", "test-title", "test-desc"));
//        await().pollInterval(Duration.ofSeconds(3)).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
//            // assert statements
//        });
//    }
}
