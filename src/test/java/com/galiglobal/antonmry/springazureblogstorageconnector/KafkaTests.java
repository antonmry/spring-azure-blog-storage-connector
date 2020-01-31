package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class KafkaTests {

    private static final String INPUT_TOPIC = "testEmbeddedIn";
    private static final String OUTPUT_TOPIC = "testEmbeddedOut";
    private static final String GROUP_NAME = "embeddedKafkaApplication";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, OUTPUT_TOPIC);

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.cloud.stream.kafka.binder.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());
    }

    @Test
    public void testSendReceive() throws IOException {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
        senderProps.put("key.serializer", ByteArraySerializer.class);
        senderProps.put("value.serializer", ByteArraySerializer.class);
        DefaultKafkaProducerFactory<byte[], byte[]> pf = new DefaultKafkaProducerFactory<>(senderProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(pf, true);
        template.setDefaultTopic(INPUT_TOPIC);
        template.sendDefault("foo".getBytes());

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(GROUP_NAME, "false", embeddedKafka.getEmbeddedKafka());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("key.deserializer", ByteArrayDeserializer.class);
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class);
        DefaultKafkaConsumerFactory<byte[], byte[]> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        Consumer<byte[], byte[]> consumer = cf.createConsumer();
        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10_000);
        consumer.commitSync();


        log.info("spring.cloud.stream.kafka.binder.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());
        assertThat(records.count()).isEqualTo(1);

        uploadToAzureStorageBlob(records.iterator().next().value());

        assertThat(new String(records.iterator().next().value())).isEqualTo("FOO");
    }

    // Azure test

    @Value("${azure.storage.account-name}")
    String accountName;

    @Value("${azure.storage.account-key}")
    String accountKey;

    @Value("${azure.storage.container-name}")
    String containerName;

    public void uploadToAzureStorageBlob(byte[] data) throws IOException {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        BlobContainerClient blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

        BlockBlobClient blobClient = blobContainerClient.getBlobClient("HelloWorld.txt").getBlockBlobClient();

        InputStream dataStream = new ByteArrayInputStream(data);

        blobClient.upload(dataStream, data.length, true);

        // Download the blob's content to output stream for debugging
        OutputStream outStream = new ByteArrayOutputStream();
        blobClient.download(outStream);
        assertThat(outStream.toString()).isEqualTo("FOO");
        outStream.close();

        dataStream.close();
    }


}

