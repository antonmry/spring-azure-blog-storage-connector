package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.microsoft.azure.spring.autoconfigure.storage.StorageProperties;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ServiceURL;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

@SpringBootApplication
public class SpringAzureBlogStorageConnectorApplication implements CommandLineRunner {
    private static final String SOURCE_FILE = "storageTestFile.txt";

    @Autowired
    private ContainerURL containerURL;

    @Autowired
    private StorageProperties properties;

    public static void main(String[] args) {
        SpringApplication.run(SpringAzureBlogStorageConnectorApplication.class);
    }

    public void run(String... var1) throws IOException {
        final File sourceFile = new File(this.getClass().getClassLoader().getResource(SOURCE_FILE).getFile());
        final File downloadFile = Files.createTempFile("azure-storage-test", null).toFile();

        StorageService.createContainer(containerURL, properties.getContainerName());
        final BlockBlobURL blockBlobURL = containerURL.createBlockBlobURL(SOURCE_FILE);

        System.out.println("Enter a command:");
        System.out.println("(P)utBlob | (G)etBlob | (D)eleteBlobs | (E)xitSample");
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8.name()));

        boolean isExit = false;
        while (!isExit) {
            System.out.println("Enter a command:");
            final String input = reader.readLine();
            if (input == null) {
                continue;
            }

            switch (input) {
                case "P":
                    StorageService.uploadFile(blockBlobURL, sourceFile);
                    break;
                case "G":
                    StorageService.downloadBlob(blockBlobURL, downloadFile);
                    break;
                case "D":
                    StorageService.deleteBlob(blockBlobURL);
                    break;
                case "E":
                    System.out.println("Cleaning up container and tmp file...");
                    containerURL.delete(null, null).blockingGet();
                    FileUtils.deleteQuietly(downloadFile);
                    isExit = true;
                    break;
                default:
                    break;
            }
        }
    }
}
