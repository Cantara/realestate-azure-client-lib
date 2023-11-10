package no.cantara.realestate.azure.tools;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.storage.AzureStorageTablesClient;
import no.cantara.realestate.azure.storage.AzureTableClient;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;


public class TrendIdsTableAdmin {
    private static final Logger log = getLogger(TrendIdsTableAdmin.class);

    public static void main(String[] args) {
        ApplicationProperties config = ApplicationProperties.builder().defaults().buildAndSetStaticSingleton();
        String connectionString = config.get(AzureStorageTablesClient.CONNECTIONSTRING_KEY);
        log.debug("ConnectionString {}", connectionString);
        String tableName = config.get("trends.lastupdated.tableName","DesigoTrendIdsLastUpdated");
        AzureTableClient azureTableClient = new AzureTableClient(connectionString, tableName);
        log.info("Found client: {}", azureTableClient);
        TableClient tableClient = azureTableClient.getTableClient();
        log.info("Found tableClient: {}", tableClient);
        tableClient.listEntities().forEach(tableEntity -> {
            Map<String, Object> properties = tableEntity.getProperties();
            log.info("TableEntity: RowKey: {}, properties: {}", tableEntity.getRowKey(), properties);
        });
        TableEntity testTrend = tableClient.getEntity("Desigo", "TestTrendTrend1");
        Map<String, Object> testTrendProperties = new HashMap<>();
        testTrendProperties.put("LastUpdatedAt", Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString());
        testTrendProperties.put("DigitalTwinSensorId", testTrend.getProperty("DigitalTwinSensorId"));
        testTrendProperties.put("DigitalTwintName", testTrend.getProperty("DigitalTwintName"));
        testTrendProperties.put("DesigoId", null);
        azureTableClient.updateRow("Desigo", "TestTrendTrend1", testTrendProperties);

//        tableClient.listEntities().forEach(tableEntity -> log.info("TableEntity: {}", tableEntity));
    }
}
