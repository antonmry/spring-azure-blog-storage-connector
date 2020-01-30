package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    public void listen(Foo2 foo) throws IOException {
        log.info("Received: " + foo);
        uploadToAzureStorageBlob(foo.toString());
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

    @Value("${azure.storage.account-name}")
    String accountName;

    @Value("${azure.storage.account-key}")
    String accountKey;

    @Value("${azure.storage.container-name}")
    String containerName;

    public void uploadToAzureStorageBlob(String data) throws IOException {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        BlobContainerClient blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

        BlockBlobClient blobClient = blobContainerClient.getBlobClient("HelloWorld.txt").getBlockBlobClient();

        InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));

        blobClient.upload(dataStream, data.length(), true);

        dataStream.close();

        // Download the blob's content to output stream.
        int dataSize = (int) blobClient.getProperties().getBlobSize();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(dataSize);
        blobClient.download(outputStream);
        outputStream.close();

        assertEquals(data, new String(outputStream.toByteArray(), StandardCharsets.UTF_8));

        // Delete the blob we created earlier.
        // blobClient.delete();

    }

}



