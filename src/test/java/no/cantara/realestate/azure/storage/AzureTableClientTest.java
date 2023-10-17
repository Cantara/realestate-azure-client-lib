package no.cantara.realestate.azure.storage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import no.cantara.config.ApplicationProperties;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.slf4j.LoggerFactory.getLogger;

class AzureTableClientTest {
    private static final Logger log = getLogger(AzureTableClientTest.class);

    public static void main(String[] args) {
        boolean useConfig = true;
        String connectionString = null;

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
        AzureTableClient azureTableClient = new AzureTableClient(connectionString, "Metasys");
        assertNotNull(azureTableClient);
        assertNotNull(azureTableClient.getTableClient());

        findNative(azureTableClient);
        List<Map<String, Object>> rows = azureTableClient.findRows("1");
        log.info("Found {} rows", rows.size());
    }

    private static void findNative(AzureTableClient azureTableClient) {
        TableClient tableClient = azureTableClient.getTableClient();
        List<String> propertiesToSelect = new ArrayList<>();
        propertiesToSelect.add("RealEstate");
        propertiesToSelect.add("Tfm");
        String partitionKey = "1";
        ListEntitiesOptions options = new ListEntitiesOptions()
                .setFilter(String.format("PartitionKey eq '%s'", partitionKey));
//                .setSelect(propertiesToSelect);

        for (TableEntity entity : tableClient.listEntities(options, null, null)) {
            Map<String, Object> properties = entity.getProperties();
            log.trace("Properties found. RealEstate {}, TFM {}",properties.get("RealEstate"), properties.get("Tfm"));
        }
    }

}