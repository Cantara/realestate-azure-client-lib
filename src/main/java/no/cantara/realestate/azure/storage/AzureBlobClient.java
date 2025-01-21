package no.cantara.realestate.azure.storage;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlockBlobSimpleUploadOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
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

    public AzureBlobClient(String connectionString, String containerName) {
        blobContainerClient = new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();

    }

    public boolean writeBlob(String blobName, String jsonContent) {
        try {
            byte[] contentBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
            InputStream dataStream = new ByteArrayInputStream(contentBytes);

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
        } catch (Exception e) {
            log.error("Failed to write blob: {}", blobName, e);
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
