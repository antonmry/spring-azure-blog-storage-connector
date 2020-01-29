package com.galiglobal.antonmry.springazureblogstorageconnector;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.util.FlowableUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Slf4j
public class StorageService {
    public static void uploadFile(BlockBlobURL blob, File sourceFile) throws IOException {
        log.info("Start uploading file %s...", sourceFile);
        final AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath());

        TransferManager.uploadFileToBlockBlob(fileChannel, blob, 8 * 1024 * 1024, null)
                .toCompletable()
                .doOnComplete(() -> log.info("File %s is uploaded.", sourceFile.toPath()))
                .doOnError(error -> log.error("Failed to upload file %s with error %s.", sourceFile.toPath(),
                        error.getMessage()))
                .blockingAwait();
    }

    public static void deleteBlob(BlockBlobURL blockBlobURL) {
        log.info("Start deleting file %s...", blockBlobURL.toURL());
        blockBlobURL.delete(null, null, null)
                .toCompletable()
                .doOnComplete(() -> log.info("Blob %s is deleted.", blockBlobURL.toURL()))
                .doOnError(error -> log.error("Failed to delete blob %s with error %s.",
                        blockBlobURL.toURL(), error.getMessage()))
                .blockingAwait();
    }

    public static void downloadBlob(BlockBlobURL blockBlobURL, File downloadToFile) {
        log.info("Start downloading file %s to %s...", blockBlobURL.toURL(), downloadToFile);
        FileUtils.deleteQuietly(downloadToFile);

        blockBlobURL.download(new BlobRange().withOffset(0).withCount(4 * 1024 * 1024L), null, false, null)
                .flatMapCompletable(
                        response -> {
                            final AsynchronousFileChannel channel = AsynchronousFileChannel
                                    .open(Paths.get(downloadToFile.getAbsolutePath()), StandardOpenOption.CREATE,
                                            StandardOpenOption.WRITE);
                            return FlowableUtil.writeFile(response.body(null), channel);
                        })
                .doOnComplete(() -> log.info("File is downloaded to %s.", downloadToFile))
                .doOnError(error -> log.error("Failed to download file from blob %s with error %s.",
                        blockBlobURL.toURL(), error.getMessage()))
                .blockingAwait();
    }

    public static void createContainer(ContainerURL containerURL, String containerName) {
        log.info("Start creating container %s...", containerName);
        try {
            final ContainerCreateResponse response = containerURL.create(null, null, null).blockingGet();
            log.info("Storage container %s created with status code: %s.", containerName, response.statusCode());
        } catch (RestException e) {
            if (e.response().statusCode() != 409) {
                log.error("Failed to create container %s.", containerName, e);
                throw e;
            } else {
                log.info("%s container already exists.", containerName);
            }
        }

    }
}
