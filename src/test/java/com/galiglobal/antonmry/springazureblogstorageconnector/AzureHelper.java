package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.*;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;

@Component
public class AzureHelper {

    public static final String AZURE_BLOB_URL = "https://%s.blob.core.windows.net";
    private BlobContainerClient blobContainerClient;
    private final BlobServiceClient blockingClient;
    private final BlobBatchClient blobBatchClient;

    @Value("${azure.storage.container-name}")
    private String containerName;

    public AzureHelper(@Value("${azure.storage.account-name}") String accountName,
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
}
