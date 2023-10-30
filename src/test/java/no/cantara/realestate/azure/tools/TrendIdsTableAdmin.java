package no.cantara.realestate.azure.tools;

import com.azure.data.tables.TableClient;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.storage.AzureStorageTablesClient;
import no.cantara.realestate.azure.storage.AzureTableClient;
import org.slf4j.Logger;

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
        tableClient.listEntities().forEach(tableEntity -> log.info("TableEntity: {}", tableEntity));

//        tableClient.listEntities().forEach(tableEntity -> log.info("TableEntity: {}", tableEntity));
    }
}
