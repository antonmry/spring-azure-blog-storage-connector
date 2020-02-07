package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

@Component
public class UploadManager {

    private BlobContainerClient blobContainerClient;

    public UploadManager(@Value("${azure.storage.account-name}") String accountName,
                         @Value("${azure.storage.account-key}") String accountKey,
                         @Value("${azure.storage.container-name}") String containerName) {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

    }

    public void upload(String filename, byte[] data) throws IOException {

        BlockBlobClient blobClient = blobContainerClient.getBlobClient(filename).getBlockBlobClient();
        InputStream dataStream = new ByteArrayInputStream(data);
        blobClient.upload(dataStream, data.length, true);
        dataStream.close();
    }

    public byte[] download(String filename) throws IOException {

        BlockBlobClient blobClient = blobContainerClient.getBlobClient(filename).getBlockBlobClient();
        int dataSize = (int) blobClient.getProperties().getBlobSize();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(dataSize);
        blobClient.download(outStream);
        outStream.close();
        return outStream.toByteArray();
    }

    // TODO: make topics and groupID a configuration property
    @KafkaListener(topics = "testEmbeddedOut", groupId = "test")
    public void listen(ConsumerRecord<?, ?> record) throws Exception {

        String filename;
        if (record.key() instanceof String) {
            filename = (String) record.key();
        } else if (record.key() instanceof byte[]) {
            filename = new String((byte[]) record.key());
        } else
            // TODO: generante UUID?
            filename = "random.txt";

        upload(filename, (byte[]) record.value());
    }


}
