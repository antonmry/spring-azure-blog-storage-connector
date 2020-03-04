package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerAsyncClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class KafkaUploaderTest {

    @Mock
    private KafkaTemplate kafkaTemplate;

    @Mock
    private BlobContainerAsyncClient blobContainerAsyncClient;

    @Mock
    ConsumerRecords<byte[], byte[]> consumerRecords;

    private UploadManager uploadManager;

    @Before
    public void init() {
        uploadManager = new UploadManager("test", "test", "test", kafkaTemplate);
        ReflectionTestUtils.setField(uploadManager, "blobContainerAsyncClient", blobContainerAsyncClient);

        ReflectionTestUtils.setField(uploadManager, "dlqTopic", "");
        ReflectionTestUtils.setField(uploadManager, "retries", 1);
        ReflectionTestUtils.setField(uploadManager, "timeout", 1000);
        ReflectionTestUtils.setField(uploadManager, "firstBackoff", 100);
        ReflectionTestUtils.setField(uploadManager, "max-backoff", 1000);
        ReflectionTestUtils.setField(uploadManager, "blockSize", 1024);
        ReflectionTestUtils.setField(uploadManager, "numBuffers", 2);
        ReflectionTestUtils.setField(uploadManager, "maxSingleUploadSize", 268435456);
        ReflectionTestUtils.setField(uploadManager, "maxRequestCount", 1);
        ReflectionTestUtils.setField(uploadManager, "maxRequestsWindowTime", 100);
    }

    @Test
    public void test() {

        ConsumerRecord<byte[], byte[]> cr1 = new ConsumerRecord<>("topic", 1, 1, "test".getBytes(), "test".getBytes());
        ConsumerRecord<byte[], byte[]> cr2 = new ConsumerRecord<>("topic", 1, 2, "test".getBytes(), "test".getBytes());

        TopicPartition topicPartition = new TopicPartition("topic", 1);

        HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> hashMap = new HashMap<>();
        hashMap.put(topicPartition, Arrays.asList(cr1, cr2));

        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(hashMap);

        StepVerifier.withVirtualTime(() -> uploadManager.process(Flux.fromIterable(consumerRecords))).expectSubscription();
    }
}

