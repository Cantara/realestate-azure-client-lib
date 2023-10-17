package no.cantara.realestate.azure.storage;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureStorageTablesClient {
    private static final Logger log = getLogger(AzureStorageTablesClient.class);

    private final TableServiceClient tableServiceClient;

    public static final String CONNECTIONSTRING_KEY = "sensormappings.azure.connectionString";


    protected AzureStorageTablesClient(TableServiceClient tableClient) {
        this.tableServiceClient = tableClient;
    }

    public AzureStorageTablesClient(String connectionString) {
        TableServiceClient tableServiceClient = new TableServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
        this.tableServiceClient = tableServiceClient;
    }

    public AzureStorageTablesClient(String storageaccountName, String accountKey, String storageTableUrl) {
        AzureNamedKeyCredential credential = new AzureNamedKeyCredential(storageaccountName, accountKey);
        TableServiceClient tableServiceClient = new TableServiceClientBuilder()
                .endpoint(storageTableUrl)
                .credential(credential)
                .buildClient();
        this.tableServiceClient = tableServiceClient;
    }

    public TableServiceClient getTableServiceClient() {
        return tableServiceClient;
    }

    public List<String> listTables() {
        List<String> tableNames = tableServiceClient.listTables().stream()
                .map(tableItem -> tableItem.getName())
                .toList();
        return tableNames;
    }

    public String getAllInTable(String tableName) {
        //return all rows in table as json
        TableClient tableClient = tableServiceClient.getTableClient(tableName);
        tableClient.listEntities().stream().forEach(tableEntity -> log.info("RowKey: {}, PartitionKey: {}, Properties: {}",
                tableEntity.getRowKey(), tableEntity.getPartitionKey(), tableEntity.getProperties()));
        return null;
    }




}
