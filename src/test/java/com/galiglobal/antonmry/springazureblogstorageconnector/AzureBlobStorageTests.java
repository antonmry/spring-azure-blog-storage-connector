package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.microsoft.azure.spring.autoconfigure.storage.StorageProperties;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Slf4j
public class AzureBlobStorageTests {

    private static final String SOURCE_FILE = "storageTestFile.txt";

    @Autowired
    private ContainerURL containerURL;

    @Autowired
    private StorageProperties properties;

    @Test
    public void testSpringAzureBlobStorage() throws Exception {

        final File sourceFile = new File(this.getClass().getClassLoader().getResource(SOURCE_FILE).getFile());
        final File downloadFile = Files.createTempFile("azure-storage-test", null).toFile();

        StorageService.createContainer(containerURL, properties.getContainerName());
        final BlockBlobURL blockBlobURL = containerURL.createBlockBlobURL(SOURCE_FILE);
        StorageService.uploadFile(blockBlobURL, sourceFile);
        StorageService.downloadBlob(blockBlobURL, downloadFile);
        StorageService.deleteBlob(blockBlobURL);
    }

    @Value("${azure.storage.account-name}")
    String accountName;

    @Value("${azure.storage.account-key}")
    String accountKey;

    @Value("${azure.storage.container-name}")
    String containerName;

    @Test
    public void testAzureStorageBlob() throws IOException {

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

        BlobServiceClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        BlobContainerClient blobContainerClient = storageClient
                .getBlobContainerClient(containerName);

        BlockBlobClient blobClient = blobContainerClient.getBlobClient("HelloWorld.txt").getBlockBlobClient();

        String data = "Hello world!";
        InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));

        blobClient.upload(dataStream, data.length());

        dataStream.close();

        // Download the blob's content to output stream.
        int dataSize = (int) blobClient.getProperties().getBlobSize();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(dataSize);
        blobClient.download(outputStream);
        outputStream.close();

        assertEquals(data, new String(outputStream.toByteArray(), StandardCharsets.UTF_8));

        // Delete the blob we created earlier.
        blobClient.delete();

    }

}

