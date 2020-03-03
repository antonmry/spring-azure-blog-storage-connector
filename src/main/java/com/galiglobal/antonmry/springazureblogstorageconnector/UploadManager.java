package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.retry.Retry;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;

@Component
// TODO: set dependencies
//@ConditionalOnProperty(prefix = "connector.azure.blob.kafka", name = "groupId")
public class UploadManager {

    @Value("${azure.storage.retries:0}")
    private int retries;

    @Value("${azure.storage.first-backoff:100}")
    private int firstBackoff;

    @Value("${azure.storage.max-backoff:1000}")
    private int maxBackoff;

    @Value("${azure.storage.block-size:1024}")
    private Integer blockSize;

    @Value("${azure.storage.num-buffers:2}")
    private Integer numBuffers;

    @Value("${azure.storage.max-single-upload-size:268435456}")
    private Integer maxSingleUploadSize;

    public static final String AZURE_BLOB_URL = "https://%s.blob.core.windows.net";

    private final ParallelTransferOptions options = new ParallelTransferOptions(blockSize, numBuffers,
            (progress) -> System.out.printf("Progress: %s%n", progress), maxSingleUploadSize);

    private final BlobServiceAsyncClient asyncClient;
    private BlobContainerAsyncClient blobContainerAsyncClient;

    public UploadManager(@Value("${azure.storage.account-name}") String accountName,
                         @Value("${azure.storage.account-key}") String accountKey,
                         @Value("${azure.storage.container-name}") String containerName) {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, AZURE_BLOB_URL, accountName);

        asyncClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildAsyncClient();

        blobContainerAsyncClient = asyncClient.getBlobContainerAsyncClient(containerName);
    }


    @Bean
    public KafkaListenerContainerFactory<?> batchFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setBatchListener(true);
        return factory;
    }

    // TODO: add transactions support https://docs.spring.io/spring-kafka/reference/html/#transactions-batch
    @KafkaListener(topics = "#{@uploadManagerConfiguration.getTopics()}",
            groupId = "#{@uploadManagerConfiguration.getGroupId()}",
            containerFactory = "batchFactory")
    public void listen(ConsumerRecords<?, ?> records) {

        // TODO: add timeout for each upload
        Flux.fromIterable(records)
                .flatMap(v -> blobContainerAsyncClient.getBlobAsyncClient(calculateFilename(v.key()))
                        .upload(Flux.just(ByteBuffer.wrap((byte[]) v.value())), options)
                        .retryWhen(
                                // Usually because blob already exists so it doesn't retry
                                Retry.onlyIf(rc -> !(rc.exception() instanceof IllegalArgumentException))
                                        .retryMax(retries)
                                        .exponentialBackoff(Duration.ofMillis(firstBackoff), Duration.ofMillis(maxBackoff))
                        )
                        // TODO: break transaction? move to DLQ?
                        .doOnError((e) -> System.out.println("Error uploading file: " + e))
                        // TODO: generate JMX metric
                        .doOnSuccess((b) -> System.out.println("Uploaded file with ETag: " + b.getETag())))
                .subscribe();
    }

    String calculateFilename(Object filename) {
        if (filename instanceof String) {
            return (String) filename;
        } else if (filename instanceof byte[]) {
            return new String((byte[]) filename);
        } else
            // TODO: generate UUID?
            return "random.txt";
    }
}
