package com.galiglobal.antonmry.springazureblogstorageconnector;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

public class KafkaHelper {

    private static Map<String, Object> producerProps;

    static {
        producerProps = new HashMap<>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", ByteArraySerializer.class);
        producerProps.put("value.serializer", ByteArraySerializer.class);
    }

    public void produce(String topic, byte[] key, byte[] value) {

        DefaultKafkaProducerFactory<byte[], byte[]> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(kafkaProducerFactory, true);
        template.send(topic, key, value);
    }

}
