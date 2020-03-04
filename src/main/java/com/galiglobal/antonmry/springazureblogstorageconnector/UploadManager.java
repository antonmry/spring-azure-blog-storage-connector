package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Retry;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;

@Component
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
public class UploadManager {

    private static Logger log = LoggerFactory.getLogger(UploadManager.class);

    @Value("${connector.azure.blob.kafka.dlq-topic:}")
    private String dlqTopic;

    @Value("${azure.storage.retries:1}")
    private int retries;

    @Value("${azure.storage.timeout:10000}")
    private int timeout;

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

    @Value("${azure.storage.max-requests-count:1}")
    private int maxRequestCount;

    @Value("${azure.storage.max-requests-window-time:100}")
    private int maxRequestsWindowTime;

    public static final String AZURE_BLOB_URL = "https://%s.blob.core.windows.net";

    private final ParallelTransferOptions options = new ParallelTransferOptions(blockSize, numBuffers,
            (progress) -> {
            }, maxSingleUploadSize);

    private BlobContainerAsyncClient blobContainerAsyncClient;

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public UploadManager(@Value("${azure.storage.account-name}") String accountName,
                         @Value("${azure.storage.account-key}") String accountKey,
                         @Value("${azure.storage.container-name}") String containerName,
                         KafkaTemplate<Object, Object> kafkaTemplate) {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, AZURE_BLOB_URL, accountName);

        BlobServiceAsyncClient asyncClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildAsyncClient();

        blobContainerAsyncClient = asyncClient.getBlobContainerAsyncClient(containerName);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public KafkaListenerContainerFactory<?> batchFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setBatchListener(true);
        return factory;
    }

    @KafkaListener(topics = "#{@uploadManagerConfiguration.getTopics()}",
            groupId = "#{@uploadManagerConfiguration.getGroupId()}",
            containerFactory = "batchFactory")
    public void listen(ConsumerRecords<?, ?> records) {

        Flux.fromIterable(records)
                .windowTimeout(maxRequestCount, Duration.ofMillis(maxRequestsWindowTime))
                .onBackpressureBuffer()
                .concatMap(a -> a.flatMap(v -> {
                            long startTime = System.currentTimeMillis();
                            return blobContainerAsyncClient.getBlobAsyncClient(calculateFilename(v.key()))
                                    .upload(Flux.just(ByteBuffer.wrap((byte[]) v.value())), options)
                                    .timeout(Mono.delay(Duration.ofMillis(timeout)))
                                    .retryWhen(
                                            // Usually because blob already exists so it doesn't retry
                                            Retry.onlyIf(rc -> {
                                                if (rc.exception().getMessage().contains("lob already exists")) {
                                                    log.debug("Duplicated file, ignoring retry " +
                                                            calculateFilename(v.key()));
                                                    return false;
                                                } else {
                                                    log.warn("Retrying upload " + calculateFilename(v.key()) +
                                                            " after error: " + rc.exception().getMessage());
                                                    return true;
                                                }
                                            })
                                                    .retryMax(retries)
                                                    .exponentialBackoff(Duration.ofMillis(firstBackoff),
                                                            Duration.ofMillis(maxBackoff))
                                    )
                                    .doOnError(e -> {
                                        if (dlqTopic.isEmpty()) {
                                            log.error("Unable to upload: " + calculateFilename(v.key()) +
                                                    " with exception: " + e);
                                        } else {
                                            log.warn("Error uploading file: " + calculateFilename(v.key()) + " with exception: " +
                                                    e);
                                            if (!e.getMessage().contains("lob already exists")) {
                                                try {
                                                    kafkaTemplate.send(dlqTopic, v.key(), v.value());
                                                } catch (Exception ex) {
                                                    log.error("Unable to publish to the DLQ: " + calculateFilename(v.key()) +
                                                            " with exception: " + ex);
                                                }
                                            }
                                        }
                                    })
                                    .doOnSuccess((b) -> log.debug("Uploaded file " + calculateFilename(v.key()) +
                                            " with ETag: " + b.getETag()))
                                    .then(Mono.defer(() -> {
                                        long diffInTime =
                                                System.currentTimeMillis() - startTime;

                                        if (diffInTime < maxRequestsWindowTime) {
                                            System.out.println("Test");
                                            return Mono.delay(Duration.ofMillis(maxRequestsWindowTime - diffInTime))
                                                    .then();
                                        }

                                        return Mono.empty();
                                    }));
                        })
                ).then()
                .doOnError(e -> log.error("Unable to upload files to Azure: " + e)).subscribe();
    }

    String calculateFilename(Object filename) {
        if (filename instanceof String) {
            return (String) filename;
        } else if (filename instanceof byte[]) {
            return new String((byte[]) filename);
        } else
            return UUID.randomUUID().toString();
    }
}
