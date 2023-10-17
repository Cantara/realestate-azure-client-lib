package no.cantara.realestate.azure.storage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureTableClient {
    private static final Logger log = getLogger(AzureTableClient.class);

    private final TableClient tableClient;

    protected AzureTableClient(TableClient tableClient) {
        this.tableClient = tableClient;
    }

    public AzureTableClient(String connectionString, String tableName) {
        TableClient tableClient = new TableClientBuilder()
                .connectionString(connectionString)
                .tableName(tableName)
                .buildClient();
        this.tableClient = tableClient;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public List<Map<String,Object>> findRows(String partitionKey) {
        ListEntitiesOptions options = new ListEntitiesOptions()
                .setFilter(String.format("PartitionKey eq '%s'", partitionKey));
        return tableClient.listEntities(options, null, null).stream()
                .map(tableEntity -> tableEntity.getProperties())
                .toList();
    }
}
