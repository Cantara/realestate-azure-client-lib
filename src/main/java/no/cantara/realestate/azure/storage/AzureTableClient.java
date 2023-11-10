package no.cantara.realestate.azure.storage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import no.cantara.realestate.RealEstateException;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureTableClient {
    private static final Logger log = getLogger(AzureTableClient.class);

    private final TableClient tableClient;

    public AzureTableClient(TableClient tableClient) {
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
            stringRows.add(stringRow);
        }
        return stringRows;
    }

    /**
     *
     * @param partitionKey
     * @param rowKey
     * @param properties
     * @throws RealEstateException if the row could not be updated.
     */
    public void updateRow(String partitionKey, String rowKey, Map<String, Object> properties) throws RealEstateException {
        try {
            TableEntity tableEntity = new TableEntity(partitionKey, rowKey);
            properties.forEach((key, value) -> {
                if (value instanceof Instant) {
                    value = ((Instant) value).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
                }
                tableEntity.addProperty(key, value);
            });
            tableClient.updateEntity(tableEntity);
        } catch (Exception e) {
            RealEstateException realEstateException = new RealEstateException("Could not update row with partitionKey " + partitionKey + " and rowKey " + rowKey + ", properties " + properties, e);
            log.trace(realEstateException.getMessage(), realEstateException);
            throw e;
        }
    }
}
