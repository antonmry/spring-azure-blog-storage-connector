package com.galiglobal.antonmry.springazureblogstorageconnector;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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

    public void produce(String topic, Object key, Object value) {
        DefaultKafkaProducerFactory<Object, Object> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<Object, Object> template = new KafkaTemplate<>(kafkaProducerFactory, true);
        template.send(topic, key, value);
    }

}
