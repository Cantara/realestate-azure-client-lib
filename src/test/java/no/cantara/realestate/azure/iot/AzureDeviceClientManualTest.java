package no.cantara.realestate.azure.iot;

import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

class AzureDeviceClientManualTest {
    private static final Logger log = getLogger(AzureDeviceClientManualTest.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            log.info("You need to provide \"primary connection string\" from a Device registered in Azure IoTHub.\n" +
                    "Enter this string as the first argument when running this class.");
        }
        String connectionString = args[0];
        log.debug("ConnectionString {}", connectionString);
        AzureDeviceClient deviceClient = new AzureDeviceClient(connectionString);
        deviceClient.openConnection();
        assertTrue(deviceClient.isConnectionEstablished());
    }

}