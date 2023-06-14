package no.cantara.realestate.azure.iot;

import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.AzureObservationDistributionClient;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

class AzureDeviceClientManualTest {
    private static final Logger log = getLogger(AzureDeviceClientManualTest.class);

    public static void main(String[] args) throws InterruptedException {
        boolean useConfig = true;
        AzureDeviceClient deviceClient;
        ApplicationProperties config;
        if (useConfig) {
            config = ApplicationProperties.builder().buildAndSetStaticSingleton();
            deviceClient = new AzureDeviceClient(config.get(AzureObservationDistributionClient.CONNECTIONSTRING_KEY));
        } else {
            if (args.length < 1) {
                log.error("You need to provide \"primary connection string\" from a Device registered in Azure IoTHub.\n" +
                        "Enter this string as the first argument when running this class.");
                System.exit(0);
            }
            String connectionString = args[0];
            log.debug("ConnectionString {}", connectionString);
            deviceClient = new AzureDeviceClient(connectionString);
        }

        deviceClient.openConnection();
        assertTrue(deviceClient.isConnectionEstablished());
        log.info("Establishing and verifying connection.");
        Thread.sleep(500);
        if (deviceClient.isConnectionEstablished()) {
            log.info("Sleeping for 10 seconds.");
            Thread.sleep(10000);
            deviceClient.closeConnection();
            assertFalse(deviceClient.isConnectionEstablished());
            log.info("DONE-Connection is closed.");
        }
    }

}