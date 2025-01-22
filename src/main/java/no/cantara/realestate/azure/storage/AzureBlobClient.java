package no.cantara.realestate.azure.storage;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlockBlobSimpleUploadOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import no.cantara.realestate.azure.CantaraRealestateAzureException;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureBlobClient {
    private static final Logger log = getLogger(AzureBlobClient.class);

    private final BlobContainerClient blobContainerClient;
    private final String containerName;

    public AzureBlobClient(String connectionString, String containerName) throws CantaraRealestateAzureException {
        blobContainerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();
        this.containerName = containerName;
        try {
            blobContainerClient.exists();
            log.debug("Blob container {} exists", containerName);
        } catch (BlobStorageException e) {
            log.error("Blob container {} does not exist", containerName, e);
            throw new CantaraRealestateAzureException("Blob container does not exist", e);
        }

    }

    public boolean writeBlob(String blobName, String jsonContent) throws CantaraRealestateAzureException {
        try {
            byte[] contentBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
            InputStream dataStream = new ByteArrayInputStream(contentBytes);
            if (blobContainerClient.exists()) {

                BlockBlobClient blobClient = blobContainerClient.getBlobClient(blobName).getBlockBlobClient();

                // Create BlobHttpHeaders and set content type
                BlobHttpHeaders headers = new BlobHttpHeaders();
                headers.setContentType("application/json");

                // Upload the blob with the specified headers
                BlockBlobSimpleUploadOptions options = new BlockBlobSimpleUploadOptions(dataStream, contentBytes.length)
                        .setHeaders(headers)
                        .setTier(AccessTier.HOT);

                blobClient.uploadWithResponse(options, Duration.ofSeconds(10), Context.NONE);

                dataStream.close();
                return true;
            } else {
                log.error("Blob container {} does not exist", blobContainerClient.getBlobContainerName());
                throw new CantaraRealestateAzureException("Blob container" + containerName + " does not exist");
            }
        } catch (BlobStorageException e) {
            log.error("Blob container {} does not exist", containerName, e);
            throw new CantaraRealestateAzureException("Blob container does not exist", e);
        } catch (Exception e) {
            log.error("Failed to write blob: {} to containerName {}", blobName, containerName, e);
            return false;
        }
    }

    public boolean writeBlobWithTags(String blobName, String jsonContent, Map<String, String> tags, Boolean overwrite) {
        try {
            byte[] contentBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
            InputStream dataStream = new ByteArrayInputStream(contentBytes);

            BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
            blobClient.upload(dataStream, contentBytes.length, overwrite);

            BlobHttpHeaders headers = new BlobHttpHeaders();
            headers.setContentType("application/json");
            blobClient.setHttpHeaders(headers);
            blobClient.setTags(tags);

            dataStream.close();
            return true;
        } catch (Exception e) {
            String logContent = jsonContent;
            if (logContent.length() > 100)
                logContent = jsonContent.substring(0,100) + "...truncated";
            log.error("Failed to write blob {}, with content {} and tags {}", blobName, logContent, tags,e);
            return false;
        }

    }
}
