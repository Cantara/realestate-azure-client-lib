package no.cantara.realestate.azure;

import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.slf4j.Logger;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

class AzureObservationDistributionClientManualTest {
    private static final Logger log = getLogger(AzureObservationDistributionClientManualTest.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            log.error("You need to provide \"primary connection string\" from a Device registered in Azure IoTHub.\n" +
                    "Enter this string as the first argument when running this class.");
            System.exit(0);
        }
        String connectionString = args[0];
        log.debug("ConnectionString {}", connectionString);
        AzureObservationDistributionClient deviceClient = new AzureObservationDistributionClient(connectionString);
        deviceClient.openConnection();
        assertTrue(deviceClient.isConnectionEstablished());
        log.info("Establishing and verifying connection.");
        Thread.sleep(500);
        if (deviceClient.isConnectionEstablished()) {
            ObservationMessage observationMessage = buildStubObservation();
            deviceClient.publish(observationMessage);
            log.info("Sleeping for 10 seconds.");
            Thread.sleep(10000);
            deviceClient.closeConnection();
            assertFalse(deviceClient.isConnectionEstablished());
            log.info("DONE-Connection is closed.");
        }
    }

    private static ObservationMessage buildStubObservation() {
        Instant observedAt = Instant.now().minusSeconds(10);
        Instant receivedAt = Instant.now();
        ObservationMessage observationMessage = new ObservationMessageBuilder()
                .withRecId("rec1")
                .withRealEstate("RE1")
                .withBuilding("Building1")
                .withFloor("04")
                .withSection("Section West")
                .withServesRoom("Room1")
                .withPlacementRoom("Room21")
                .withClimateZone("air1")
                .withElectricityZone("light")
                .withSensorType("temp")
                .withMeasurementUnit("C")
                .withObservedValue(22)
                .withObservedAt(observedAt)
                .withReceivedAt(receivedAt)
                .withTfm("TFM12345")
                .build();
        return observationMessage;
    }

}