package no.cantara.realestate.azure.dataexplorer;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import no.cantara.config.ApplicationProperties;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureDataExplorerClient {
    private static final Logger log = getLogger(AzureDataExplorerClient.class);

    public static final String DEFAULT_DATABASE = "default";
    private final Client kustoClient;
    private final String database;

    public AzureDataExplorerClient(Client kustoClient, String database) {
        this.kustoClient = kustoClient;
        this.database = database;
    }

    public AzureDataExplorerClient(ApplicationProperties config) throws Exception {
        String clusterUri = config.get("azure.dataexplorer.clusterUri");
        String applicationId = config.get("azure.dataexplorer.appId");
        String applicationKey = config.get("azure.dataexplorer.appSecret");
        String tenantId = config.get("azure.dataexplorer.tenantId");
        String database = config.get("azure.dataexplorer.database", "default");
        ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, applicationId, applicationKey, tenantId);
//            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithUserPrompt(clusterUri);
        Client kustoClient = ClientFactory.createClient(kcsb);
        this.kustoClient = kustoClient;
        this.database = database;
    }

    public AzureDataExplorerClient(String clusterUri, String applicationId, String applicationKey, String tenantId, String database) throws Exception {
        ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUri, applicationId, applicationKey, tenantId);
//            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithUserPrompt(clusterUri);
        Client kustoClient = ClientFactory.createClient(kcsb);
        this.kustoClient = kustoClient;
        this.database = database;
    }


    protected KustoResultSetTable runQuery(String query) {
        KustoResultSetTable primaryResults = null;
        try {
            KustoOperationResult response = kustoClient.executeQuery(database, query);
            primaryResults = response.getPrimaryResults();
        } catch (ThrottleException e) {
            log.warn("Throttled. Waiting 10 seconds before retrying", e);
            try {
                Thread.sleep(1000);
                log.info("Try to re-run query: {}", query);
                runQuery(query);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } catch (DataServiceException e) {
            throw new RuntimeException(e);
        } catch (DataClientException e) {
            throw new RuntimeException(e);
        }
        return primaryResults;
    }

    protected KustoResultSetTable runMgmtQuery(String query) {
        KustoResultSetTable primaryResults = null;
        try {
            KustoOperationResult response = kustoClient.executeMgmt(database, query);
            primaryResults = response.getPrimaryResults();
            while (primaryResults.next()) {
                KustoResultColumn[] columns = primaryResults.getColumns();
                for (KustoResultColumn column : columns) {
                    log.debug(column.getColumnName() + ": " + primaryResults.getString(column.getColumnName()) + ", ");
                }
            }
        } catch (ThrottleException e) {
            log.warn("Throttled. Waiting 10 seconds before retrying", e);
            try {
                Thread.sleep(10000);
                log.info("Try to re-run query: {}", query);
                runMgmtQuery(query);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } catch (DataServiceException e) {
            throw new RuntimeException(e);
        } catch (DataClientException e) {
            throw new RuntimeException(e);
        }
        return primaryResults;
    }

    /**
     * Executes a query and returns results as a list of maps.
     * Each map represents a row with column names as keys.
     *
     * @param primaryResults
     * @return list of rows, where each row is a map of column name to value
     */
    public List<Map<String, Object>> mapToList(KustoResultSetTable primaryResults) {
        List<Map<String, Object>> mappedList = new ArrayList<>();
        try {
            KustoResultColumn[] columns = primaryResults.getColumns();
            while (primaryResults.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (KustoResultColumn column : columns) {
                    row.put(column.getColumnName(), primaryResults.getObject(column.getColumnName()));
                }
                mappedList.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error mapping results to list", e);
        }
        return mappedList;
    }


    public boolean validateResult(KustoResultSetTable result, String expectedString) {
        if (result == null || expectedString == null) {
            log.trace("Result set or expected string is null");
            return false;
        }
        try {
            while (result.next()) {
                KustoResultColumn[] columns = result.getColumns();
                for (KustoResultColumn column : columns) {
                    try {
                        if (column.getColumnName() != null && result.getObject(column.getColumnName()) != null) {
                            String value = result.getString(column.getColumnName());
                            log.info("{} - {}: {}", expectedString, column.getColumnName(), value);
                        } else {
                            log.warn("{} - Column is null or result.getObject is null", expectedString);
                        }
                    } catch (NullPointerException e) {
                        log.warn("{} - NullPointerException while processing column. Reason: {}", expectedString, e);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            log.error("{} - Failed to process result set. Reason: {}", expectedString, e);
            throw new RuntimeException(e);
        }
    }

    public String resultsetToString(KustoResultSetTable result) {
        if (result == null) {
            return "KustoResultSetTable is null";
        }
        StringBuilder sb = new StringBuilder();
        try {
            if (result.hasNext()) {
                while (result.next()) {
                    KustoResultColumn[] columns = result.getColumns();
                    for (KustoResultColumn column : columns) {
                        sb.append(column.getColumnName()).append(": ").append(result.getString(column.getColumnName())).append(", ");
                    }
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            log.error("Failed to convert result set to string. Reason: {}", e);
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

}
