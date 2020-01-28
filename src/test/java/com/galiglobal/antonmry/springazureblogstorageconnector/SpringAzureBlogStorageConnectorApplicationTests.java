package com.galiglobal.antonmry.springazureblogstorageconnector;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
class SpringAzureBlogStorageConnectorApplicationTests {

    @Container
    public KafkaContainer kafkaContainer = new KafkaContainer();

    @Test
    void shouldBeRunningKafka() {
        assertTrue(kafkaContainer.isRunning());
    }

    @Test
    void contextLoads() {
    }

}
