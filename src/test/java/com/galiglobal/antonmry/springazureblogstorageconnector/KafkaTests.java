package com.galiglobal.antonmry.springazureblogstorageconnector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class KafkaTests {

    private static final String TOPIC = "testEmbeddedOut";
    private static final String GROUP_NAME = "embeddedKafkaApplication";

    @Autowired
    private AzureBlobStorageManager azure;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC);

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.cloud.stream.kafka.binder.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void testSendReceive() throws IOException {
        KafkaManager kafkaManager = new KafkaManager(
                KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka()),
                KafkaTestUtils.consumerProps(GROUP_NAME, "false", embeddedKafka.getEmbeddedKafka())
        );

        kafkaManager.produce(TOPIC, "foo.txt".getBytes(), "foo".getBytes());

        ConsumerRecords<byte[], byte[]> records = kafkaManager.consume(TOPIC);
        assertThat(records.count()).isEqualTo(1);

        ConsumerRecord<byte[], byte[]> record = records.iterator().next();
        assertThat(new String(record.value())).isEqualToIgnoringCase("FOO");

        String filename = new String(record.key());
        azure.upload(filename, record.value());
        assertThat(new String(azure.download(filename))).isEqualToIgnoringCase("FOO");
    }
}

