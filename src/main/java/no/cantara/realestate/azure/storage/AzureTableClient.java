package no.cantara.realestate.azure.storage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
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
        //Cast from Map<String, Object>     to Map<String, String>
        return tableClient.listEntities(options, null, null).stream()
                .map(tableEntity -> tableEntity.getProperties())
                .toList();
    }

    public List<Map<String,String>> listRows(String partitionKey) {
        List<Map<String,String>> stringRows = new ArrayList<>();
        List<Map<String, Object>> rows = findRows(partitionKey);
        for (Map<String, Object> row : rows) {
            Map<String,String> stringRow = new HashMap<>();
            for (String key : row.keySet()) {
                Object value = row.get(key);
                if (value instanceof String) {
                    stringRow.put(key, (String) value);
                } else {
                    try {
                        stringRow.put(key, value.toString());
                    } catch (Exception e) {
                        log.error("Could not convert value {} to String", value);
                        stringRow.put(key, "--FailedInConversionFromAzureTable--");
                    }
                }
            }
        }
        return stringRows;
    }
}
