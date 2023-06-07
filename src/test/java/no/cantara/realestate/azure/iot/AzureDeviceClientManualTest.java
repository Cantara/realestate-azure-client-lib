package no.cantara.realestate.azure.iot;

import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

class AzureDeviceClientManualTest {
    private static final Logger log = getLogger(AzureDeviceClientManualTest.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            log.info("You need to provide \"primary connection string\" from a Device registered in Azure IoTHub.\n" +
                    "Enter this string as the first argument when running this class.");
        }
        String connectionString = args[0];
        log.debug("ConnectionString {}", connectionString);
        AzureDeviceClient deviceClient = new AzureDeviceClient(connectionString);
        deviceClient.openConnection();
        assertTrue(deviceClient.isConnectionEstablished());
        log.info("Establishing and verifying connection.");
        Thread.sleep(500);
        if (deviceClient.isConnectionEstablished()) {
            log.info("Sleeping for 10 seconds before sleeping.");
            Thread.sleep(10000);
            deviceClient.closeConnection();
            assertFalse(deviceClient.isConnectionEstablished());
            log.info("DONE-Connection is closed.");
        }
    }

}