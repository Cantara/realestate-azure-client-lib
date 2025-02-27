package no.cantara.realestate.azure.storage;

import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.CantaraRealestateAzureException;
import org.slf4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

class ManualAzureBlobClientTest {
    private static final Logger log = getLogger(ManualAzureBlobClientTest.class);

    private AzureBlobClient azureBlobClient;
    private static String containerName = "metasyscrawler";

    public static void main(String[] args) {
        boolean useConfig = true;
        String connectionString = getConnectionString(args, useConfig);
        String fileName = "test1234.json";

        try {
            AzureBlobClient azureBlobClient = new AzureBlobClient(connectionString, containerName);
            //writeFileToAzure(azureBlobClient, fileName);
//            List<String> filenames = findFilesByTag(azureBlobClient, "parentId", "parent1234");
//            fileName = filenames.get(0);
//            readFileFromAzure(azureBlobClient, fileName);
            listBlobsUpdatedToday(azureBlobClient);
        } catch (CantaraRealestateAzureException e) {
            log.info("Failed write file to containerName: {}", containerName, e);
        } catch (Exception e) {
            CantaraRealestateAzureException cantaraRealestateAzureException = new CantaraRealestateAzureException("Failed to write blob to containerName: " + containerName, e);
            log.info(cantaraRealestateAzureException.getMessage());
        } finally {
            log.info("Done");
        }

    }
    private static void listBlobsUpdatedToday(AzureBlobClient azureBlobClient) {
        Instant now = Instant.now();
        Instant from = now.truncatedTo(ChronoUnit.DAYS);
        Set<String> blobNames = azureBlobClient.findBlobsUpdatedByDate(from, now);
        for (String blobName : blobNames) {
            log.debug("blobName: {}", blobName);
            System.out.println(blobName);
        }
    }

    private static void writeFileToAzure(AzureBlobClient azureBlobClient, String fileName) {
        Map<String, String> tags = new HashMap<>();
        tags.put("test", "true");
        Boolean overwrite = true;
        log.debug("Length of longJson: {}", longJson.length());
        azureBlobClient.writeBlobWithTags(fileName, longJson, tags, overwrite);
    }
    private static void readFileFromAzure(AzureBlobClient azureBlobClient, String fileName) {
        String jsonContent = azureBlobClient.readBlob(fileName);
        log.debug("jsonContent: {}", jsonContent);
        System.out.println(jsonContent);
    }
    private static List<String> findFilesByTag(AzureBlobClient azureBlobClient, String tagKey, String tagValue) {
        return azureBlobClient.findBlobsByTags(tagKey, tagValue);
    }

    private static String getConnectionString(String[] args, boolean useConfig) {
        String connectionString;
        if (useConfig) {
            ApplicationProperties config = ApplicationProperties.builder().defaults().buildAndSetStaticSingleton();
            connectionString = config.get(AzureStorageTablesClient.CONNECTIONSTRING_KEY);
            log.debug("ConnectionString {}", connectionString);

        } else {
            if (args.length < 1) {
                log.error("You need to provide \"primary connection string\" from a Storage Account Table registered in Azure IoTHub.\n" +
                        "Enter this string as the first argument when running this class.");
                System.exit(0);
            }
            connectionString = args[0];
            log.debug("ConnectionString from commandLine {}", connectionString);
        }
        return connectionString;
    }

    private static String longJson = """
    {
  "self": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502?includeSchema=false&viewId=viewNameEnumSet.focusView",
  "objectType": "objectTypeEnumSet.bacnetAvClass",
  "parentUrl": "http://localhost:50051/api/v4/objects/d37834ae-77ac-5592-92e6-0c6de7af4b67",
  "objectsUrl": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502/objects",
  "networkDeviceUrl": "http://localhost:8080/api/v4/networkDevices/bda05420-7ced-5dbb-a80f-eac2936cd7c7",
  "pointsUrl": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502/points",
  "trendedAttributesUrl": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502/trendedAttributes",
  "alarmsUrl": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502/alarms",
  "auditsUrl": "http://localhost:50051/api/v4/objects/4afbbb72-f84f-4a43-acab-b29cba839502/audits",
  "item": {
    "id": "4afbbb72-f84f-4a43-acab-b29cba839502",
    "name": "+1=563.01-RY01001",
    "description": "Aktuelt Co2 nivå",
    "bacnetObjectType": "objectTypeEnumSet.bacAvClass",
    "objectCategory": "objectCategoryEnumSet.hvacCategory",
    "reliability": "reliabilityEnumSet.reliable",
    "alarmState": "objectStatusEnumSet.osNormal",
    "overrideExpirationTime": {
      "date": null,
      "time": null
    },
    "itemReference": "TODO",
    "version": {
      "major": 1,
      "minor": 0
    },
    "relinquishDefault": 0,
    "units": "unitEnumSet.partsPerMillion",
    "displayPrecision": "displayPrecisionEnumSet.displayPrecisionPt1",
    "covIncrement": 0,
    "presentValue": 420.799988,
    "status": "objectStatusEnumSet.osNormal",
    "attrChangeCount": 33,
    "defaultAttribute": "attributeEnumSet.presentValue"
  },
  "effectivePermissions": {
    "canDelete": true,
    "canModify": true
  },
  "views": [
    {
      "title": "Focus",
      "views": [
        {
          "title": "Basic",
          "views": [
            {
              "title": "Object",
              "properties": [
                "name",
                "description",
                "bacnetObjectType",
                "objectCategory"
              ],
              "id": "viewGroupEnumSet.objectGrp"
            },
            {
              "title": "Status",
              "properties": [
                "reliability",
                "alarmState",
                "overrideExpirationTime"
              ],
              "id": "viewGroupEnumSet.statusGrp"
            }
          ],
          "id": "groupTypeEnumSet.basicGrpType"
        },
        {
          "title": "Advanced",
          "views": [
            {
              "title": "Engineering Values",
              "properties": [
                "itemReference",
                "version",
                "relinquishDefault",
                "id"
              ],
              "id": "viewGroupEnumSet.engValuesGrp"
            },
            {
              "title": "Display",
              "properties": [
                "units",
                "displayPrecision",
                "covIncrement"
              ],
              "id": "viewGroupEnumSet.displayGrp"
            }
          ],
          "id": "groupTypeEnumSet.advancedGrpType"
        },
        {
          "title": "Key",
          "views": [
            {
              "title": "None",
              "properties": [
                "presentValue",
                "status",
                "attrChangeCount",
                "defaultAttribute"
              ],
              "id": "viewGroupEnumSet.noGrp"
            }
          ],
          "id": "groupTypeEnumSet.keyGrpType"
        }
      ],
      "id": "viewNameEnumSet.focusView"
    }
  ],
  "condition": {
    "presentValue": {
      "priority": "writePriorityEnumSet.priorityDefault"
    }
  }
}

""";
}