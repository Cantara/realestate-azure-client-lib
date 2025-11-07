package no.cantara.realestate.azure.storage;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.TaggedBlobItem;
import com.azure.storage.blob.options.BlockBlobSimpleUploadOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import no.cantara.realestate.azure.CantaraRealestateAzureException;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

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
            log.info("Blob container {} does not exist", containerName, e);
            throw new CantaraRealestateAzureException("Blob container does not exist", e);
        } catch (Exception e) {
            log.info("Failed to write blob: {} to containerName {}", blobName, containerName, e);
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
            log.info("Failed to write blob {}, with content {} and tags {}", blobName, logContent, tags,e);
            return false;
        }
    }
    public boolean writeZipBlobWithTags(String blobName, byte[] zipContent, Map<String, String> tags, Boolean overwrite) {
        try {
            InputStream dataStream = new ByteArrayInputStream(zipContent);

            BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
            blobClient.upload(dataStream, zipContent.length, overwrite);

            BlobHttpHeaders headers = new BlobHttpHeaders();
            headers.setContentType("application/zip");
            headers.setContentEncoding("base64");
            blobClient.setHttpHeaders(headers);
            blobClient.setTags(tags);

            dataStream.close();
            return true;
        } catch (Exception e) {
            log.info("Failed to write zip blob: {} to containerName {}", blobName, containerName, e);
            return false;
        }
    }
    //Read blob from container
    public String readBlob(String blobName) {
        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
            return blobClient.downloadContent().toString();
        } catch (Exception e) {
            log.info("Failed to read blob: {} from containerName {}", blobName, containerName, e);
            return null;
        }
    }

    public List<String> findBlobsByTags(String tagKey, String tagValue) {
        List<String> blobNames = new ArrayList<>();
        try {
            String query = String.format("\"%s\" = '%s'", tagKey, tagValue);
            Iterable<TaggedBlobItem> blobs = blobContainerClient.findBlobsByTags(query);
            for (TaggedBlobItem blob : blobs) {
                blobNames.add(blob.getName());
            }
        } catch (Exception e) {
            log.info("Failed to find blobs by tags: {}={}", tagKey, tagValue, e);
        }
        return blobNames;
    }

    public Set<String> findAllTagNames() {
        Set<String> uniqueTagNames = new HashSet<>();
        try {
            Iterable<TaggedBlobItem> blobs = blobContainerClient.findBlobsByTags("1=1"); // Query to get all blobs
            for (TaggedBlobItem blob : blobs) {
                Map<String, String> tags = blob.getTags();
                uniqueTagNames.addAll(tags.keySet());
            }
        } catch (Exception e) {
            log.info("Failed to find all unique tag names", e);
        }
        return uniqueTagNames;
    }

    public Set<String> findAllBlobNames() {
        Set<String> blobNames = new HashSet<>();
        try {
            blobContainerClient.listBlobs().forEach(blobItem -> blobNames.add(blobItem.getName()));
        } catch (Exception e) {
            log.info("Failed to find all blob names", e);
        }
        return blobNames;
    }

    public Set<String> findBlobsUpdatedByDate(Instant fromDate, Instant toDate) {
        Set<String> blobNames = new HashSet<>();
        try {
            blobContainerClient.listBlobs().forEach(blobItem -> {
                Instant lastModified = blobItem.getProperties().getLastModified().toInstant();
                if ((lastModified.isAfter(fromDate) || lastModified.equals(fromDate)) &&
                        (lastModified.isBefore(toDate) || lastModified.equals(toDate))) {
                    blobNames.add(blobItem.getName());
                }
            });
        } catch (Exception e) {
            log.info("Failed to find blobs updated between {} and {}", fromDate, toDate, e);
        }
        return blobNames;
    }

    public String getBlobContentAsString(String blobName) {
        String content = null;
        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
            BinaryData data = blobClient.downloadContent();
            log.debug("Found blob with name: {}", blobName);
            if (data != null) {
                log.debug("Blob {} has content", blobName);
                content = data.toString();
            }
        } catch (Exception e) {
            log.info("Failed to get blob by name: {}", blobName, e);
        }
        return content;
    }
}
