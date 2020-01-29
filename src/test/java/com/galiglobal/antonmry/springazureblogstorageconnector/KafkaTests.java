package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@EmbeddedKafka(topics = "topic1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@SpringBootTest
@Slf4j
public class KafkaTests {

    @Autowired
    private KafkaTemplate<String, String> template;

    final CountDownLatch latch = new CountDownLatch(4);

    @KafkaListener(id = "foo", topics = "topic1")
    public void listen1(String message) {
        log.info("received: " + message);
        latch.countDown();
    }

    @Test
    public void testKafka() throws Exception {

        // Send
        template.setDefaultTopic("topic1");
        template.sendDefault("0", "foo");
        template.sendDefault("2", "bar");
        template.sendDefault("0", "baz");
        template.sendDefault("2", "qux");
        template.flush();
        assertTrue(latch.await(60, TimeUnit.SECONDS));
    }

}

