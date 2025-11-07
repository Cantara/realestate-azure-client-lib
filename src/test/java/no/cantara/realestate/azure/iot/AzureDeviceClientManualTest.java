package no.cantara.realestate.azure.iot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.MessageType;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.AzureObservationDistributionClient;
import no.cantara.realestate.azure.rec.RecObservationMessage;
import no.cantara.realestate.json.RealEstateObjectMapper;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

class AzureDeviceClientManualTest {
    private static final Logger log = getLogger(no.cantara.realestate.azure.iot.AzureDeviceClientManualTest.class);

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        boolean useConfig = true;
        AzureDeviceClient deviceClient;
        ApplicationProperties config;
        if (useConfig) {
            config = ApplicationProperties.builder()
                    .defaults()
                    .buildAndSetStaticSingleton();

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
            ObservationMessage observationMessage = createTestMessage();
            MessageSentCallback callback = new MessageSentCallback() {
                @Override
                public void onMessageSent(Message sentMessage,
                                          IotHubClientException exception,
                                          Object context) {
                    if (exception != null) {
                        System.err.println("Send failed: " + exception.getMessage());
                    } else {
                        System.out.println("Message sent successfully.");
                    }
                }
            };
            Message message = buildTelemetryMessage(observationMessage);
            deviceClient.sendEventAsync(message, callback);
            log.info("Sleeping for 10 seconds.");
            Thread.sleep(10000);
            deviceClient.closeConnection();
            assertFalse(deviceClient.isConnectionEstablished());
            log.info("DONE-Connection is closed.");
        }
    }

    protected static Message buildTelemetryMessage(ObservationMessage observationMessage) throws JsonProcessingException {

        RecObservationMessage recObservationMessage = new RecObservationMessage(observationMessage);
        String observationJson = RealEstateObjectMapper.getInstance().getObjectMapper().writeValueAsString(recObservationMessage);
        log.trace("Publishing RecObservationMessage: {}", observationJson);
        Message telemetryMessage = new Message(observationJson);
        String messageId = UUID.randomUUID().toString();
        telemetryMessage.setMessageId(messageId);
        telemetryMessage.setMessageType(MessageType.DEVICE_TELEMETRY);
        telemetryMessage.setContentEncoding(StandardCharsets.UTF_8.name());
        telemetryMessage.setContentType("application/json");
        return telemetryMessage;
    }

    private static ObservationMessage createTestMessage() {
        final Random random = new Random();
        Instant now = Instant.now();
        String sensorId = "stability-test-" + random.nextInt(5);
        double temperature = 18.0 + random.nextDouble() * 12; // 18-30°C

        return new ObservationMessageBuilder()
                .withSensorId(sensorId)
                .withRealEstate("StabilityTestRE")
                .withBuilding("TestBuilding")
                .withFloor(String.format("%02d", 1 + random.nextInt(3)))
                .withSection("Section-" + (char)('A' + random.nextInt(3)))
                .withServesRoom("Room-" + (100 + random.nextInt(10)))
                .withSensorType("temp")
                .withMeasurementUnit("C")
                .withValue(temperature)
                .withObservationTime(now.minusSeconds(random.nextInt(30)))
                .withReceivedAt(now)
                .withTfm("TFM-STABILITY-TEST")
                .build();
    }

}