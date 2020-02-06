package com.galiglobal.antonmry.springazureblogstorageconnector;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
public class KafkaManager {

    // TODO: move props out of here
    private Map<String, Object> senderProps;
    private Map<String, Object> consumerProps;

    public KafkaManager(Map<String, Object> senderProps, Map<String, Object> consumerProps) {
        this.senderProps = senderProps;
        this.consumerProps = consumerProps;

        this.senderProps.put("key.serializer", ByteArraySerializer.class);
        this.senderProps.put("value.serializer", ByteArraySerializer.class);

        this.consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumerProps.put("key.deserializer", ByteArrayDeserializer.class);
        this.consumerProps.put("value.deserializer", ByteArrayDeserializer.class);
    }

    public void produce(String topic, byte[] key, byte[] value) {

        DefaultKafkaProducerFactory<byte[], byte[]> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(kafkaProducerFactory, true);
        template.send(topic, key, value);
    }

    public ConsumerRecords<byte[], byte[]> consume(String topic) {

        DefaultKafkaConsumerFactory<byte[], byte[]> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        Consumer<byte[], byte[]> consumer = cf.createConsumer();
        consumer.subscribe(Collections.singleton(topic));
        // TODO: fix this
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10_000);
        consumer.commitSync();
        return records;
    }
}
