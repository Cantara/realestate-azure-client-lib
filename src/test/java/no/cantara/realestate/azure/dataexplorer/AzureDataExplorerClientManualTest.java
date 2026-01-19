package no.cantara.realestate.azure.dataexplorer;

import com.microsoft.azure.kusto.data.KustoResultSetTable;
import no.cantara.config.ApplicationProperties;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

class AzureDataExplorerClientManualTest {
    private static final Logger log = getLogger(AzureDataExplorerClientManualTest.class);


    public static void main(String[] args) {
        try {
            ApplicationProperties config = ApplicationProperties.builder().defaults().buildAndSetStaticSingleton();
            String clusterUri = config.get("azure.dataexplorer.clusterUri");
            String applicationId = config.get("azure.dataexplorer.appId");
            String applicationKey = config.get("azure.dataexplorer.appSecret");
            String tenantId = config.get("azure.dataexplorer.tenantId");
            String database = config.get("azure.dataexplorer.database","default");
            AzureDataExplorerClient dataExplorerClient = new AzureDataExplorerClient(clusterUri, applicationId, applicationKey, tenantId, database);
            String query = "observations | take 1";
            KustoResultSetTable result = dataExplorerClient.runQuery(query);
            log.info("Query executed successfully. Retrieved results: {}", dataExplorerClient.resultsetToString(result));
            if (dataExplorerClient.validateResult(result, "true")) {
                log.info("Validation successful: 'true' found in results.");
            } else {
                log.warn("Validation failed: 'true' not found in results.");
            }

        } catch (Exception e) {
            log.error("Failed to create Azure Data Explorer client: {}", e.getMessage(), e);
        }
    }

}