package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.backoff.FixedBackOff;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@EmbeddedKafka(topics = {"topic1", "topic1.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        controlledShutdown = true)
@SpringBootTest
@Slf4j
public class KafkaTests {

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            KafkaTemplate<Object, Object> template) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(
                // dead-letter after 3 tries
                new DeadLetterPublishingRecoverer(template), new FixedBackOff(0L, 2)));
        factory.setMessageConverter(new StringJsonMessageConverter());
        return factory;
    }

    @Bean
    public RecordMessageConverter converter() {
        return new StringJsonMessageConverter();
    }

    final CountDownLatch latch = new CountDownLatch(4);

    @KafkaListener(id = "fooGroup", topics = "topic1", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Foo2 foo) {
        log.info("Received: " + foo);
        latch.countDown();
    }

    @KafkaListener(id = "dltGroup", topics = "topic1.DLT")
    public void dltListen(String in) {
        log.info("Received from DLT: " + in);
    }


    @Test
    public void testKafka() throws Exception {

        // Send
        template.send("topic1", new Foo2("foo"));
        template.send("topic1", new Foo2("bar"));
        template.send("topic1", new Foo2("baz"));
        template.send("topic1", new Foo2("qux"));
        template.flush();
        assertTrue(latch.await(60, TimeUnit.SECONDS));
    }

}

