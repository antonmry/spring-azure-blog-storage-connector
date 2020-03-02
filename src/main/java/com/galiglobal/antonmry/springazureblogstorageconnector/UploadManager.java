package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.*;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

@Component
// TODO: set dependencies
//@ConditionalOnProperty(prefix = "connector.azure.blob.kafka", name = "groupId")
public class UploadManager {

    // TODO: separate Azure methods used only for testing for a separated class
    public static final String AZURE_BLOB_URL = "https://%s.blob.core.windows.net";
    private BlobContainerClient blobContainerClient;

    private final ParallelTransferOptions options = new ParallelTransferOptions(1096, 4,
            (progress) -> System.out.printf("Progress: %s%n", progress),
            BlockBlobAsyncClient.MAX_UPLOAD_BLOB_BYTES);


    @Value("${azure.storage.container-name}")
    private String containerName;

    private final BlobServiceClient blockingClient;
    private final BlobBatchClient blobBatchClient;
    private final BlobServiceAsyncClient asyncClient;
    private BlobContainerAsyncClient blobContainerAsyncClient;

    public UploadManager(@Value("${azure.storage.account-name}") String accountName,
                         @Value("${azure.storage.account-key}") String accountKey,
                         @Value("${azure.storage.container-name}") String containerName) {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, AZURE_BLOB_URL, accountName);

        blockingClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        blobContainerClient = blockingClient.getBlobContainerClient(containerName);
        blobBatchClient = new BlobBatchClientBuilder(blockingClient).buildClient();


        asyncClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildAsyncClient();

        blobContainerAsyncClient = asyncClient.getBlobContainerAsyncClient(containerName);

    }

    public void upload(String filename, byte[] data) throws IOException {

        BlockBlobClient blobClient = blobContainerClient.getBlobClient(filename).getBlockBlobClient();
        InputStream dataStream = new ByteArrayInputStream(data);
        blobClient.upload(dataStream, data.length, true);
        dataStream.close();
    }

    public void deleteBatch(List<String> filenames) {

        BlobBatch blobBatch = blobBatchClient.getBlobBatch();

        for (String filename : filenames) {
            blobBatch.deleteBlob(containerName, filename);
        }

        // Note: we aren't reviewing here if requests fail. Use only for testing
        blobBatchClient.submitBatch(blobBatch);

    }

    public byte[] download(String filename) throws IOException {

        BlockBlobClient blobClient = blobContainerClient.getBlobClient(filename).getBlockBlobClient();
        int dataSize = (int) blobClient.getProperties().getBlobSize();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(dataSize);
        blobClient.download(outStream);
        outStream.close();
        return outStream.toByteArray();
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
    public void listen(ConsumerRecords<?, ?> records) throws Exception {

        Flux.fromIterable(records)
                .flatMap(v -> blobContainerAsyncClient.getBlobAsyncClient(calculateFilename(v.key()))
                        .upload(Flux.just(ByteBuffer.wrap((byte[]) v.value())), options)
                        .doOnError((e) -> {
                            // TODO: manage java.lang.IllegalArgumentException: Blob already exists. Specify overwrite to true to force update the blob.
                            System.out.println("Error uploading file: " + e);
                        })
                        .doOnSuccess((b) -> System.out.println("Uploaded file with ETag: " + b.getETag())))
                .subscribe();
    }

    String calculateFilename(Object filename) {
        if (filename instanceof String) {
            return (String) filename;
        } else if (filename instanceof byte[]) {
            return new String((byte[]) filename);
        } else
            // TODO: generante UUID?
            return "random.txt";
    }
}
