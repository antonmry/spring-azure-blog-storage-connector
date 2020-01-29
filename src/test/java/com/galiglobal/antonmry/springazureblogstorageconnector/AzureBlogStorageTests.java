package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.microsoft.azure.spring.autoconfigure.storage.StorageProperties;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.io.File;
import java.nio.file.Files;

@SpringBootTest
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Slf4j
public class AzureBlogStorageTests {

    private static final String SOURCE_FILE = "storageTestFile.txt";

    @Autowired
    private ContainerURL containerURL;

    @Autowired
    private StorageProperties properties;

    @Test
    public void testAzureBlogStorage() throws Exception {

        final File sourceFile = new File(this.getClass().getClassLoader().getResource(SOURCE_FILE).getFile());
        final File downloadFile = Files.createTempFile("azure-storage-test", null).toFile();

        StorageService.createContainer(containerURL, properties.getContainerName());
        final BlockBlobURL blockBlobURL = containerURL.createBlockBlobURL(SOURCE_FILE);
        StorageService.uploadFile(blockBlobURL, sourceFile);
        StorageService.downloadBlob(blockBlobURL, downloadFile);
        StorageService.deleteBlob(blockBlobURL);

    }

}

