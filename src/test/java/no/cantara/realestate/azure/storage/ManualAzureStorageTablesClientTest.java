package no.cantara.realestate.azure.storage;

import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import no.cantara.config.ApplicationProperties;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

class ManualAzureStorageTablesClientTest {
    private static final Logger log = getLogger(ManualAzureStorageTablesClientTest.class);

    public static void main(String[] args) {
        boolean useConfig = true;
        AzureStorageTablesClient storageTablesClient;
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
        buildNative(connectionString);

        storageTablesClient = new AzureStorageTablesClient(connectionString);
        log.info("Found client: {}", storageTablesClient);

        log.info("Listing tables");
        storageTablesClient.listTables().stream().forEach(tableItem -> log.info("Table name: {}", tableItem));

    }

    private static void buildNative(String connectionString) {
        TableServiceClient tableServiceClient = new TableServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
        log.info("Found tableServiceClient: {}", tableServiceClient);
        tableServiceClient.listTables().forEach(tableItem -> log.info("Table name: {}", tableItem.getName()));
    }
}