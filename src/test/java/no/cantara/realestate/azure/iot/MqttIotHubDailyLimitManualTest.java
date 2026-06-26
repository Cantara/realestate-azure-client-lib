package no.cantara.realestate.azure.iot;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageType;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.azure.AzureObservationDistributionClient;
import no.cantara.realestate.azure.rec.RecObservationMessage;
import no.cantara.realestate.json.RealEstateObjectMapper;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Verify the situation when daily rate limit is reached.
 * run with -Dlogback.configurationFile=./src/test/resources/logback-test.xml
 */
class MqttIotHubDailyLimitManualTest {
    private static final Logger log = getLogger(MqttIotHubDailyLimitManualTest.class);

    public static final int DAILY_LIMIT = 400000;
    public static final int BATCH_SIZE = 1000;
    private static final String SIMULATOR_CONNECTIONSTRING_KEY = "simulator.azure.connectionString";

    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        boolean useConfig = true;
        boolean runInBatch = true;
        int cleanupPeriodSec = 240;
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger("no.cantara").setLevel(Level.INFO);
        context.getLogger("no.cantara.realestate.azure.iot.MqttIotHubDailyLimitManualTest").setLevel(Level.TRACE);
        context.getLogger("com.microsoft.azure.sdk.iot").setLevel(Level.ERROR);

        AzureObservationDistributionClient distributionClient = createClient(args, useConfig);
        distributionClient.openConnection();
        assertTrue(distributionClient.isConnectionEstablished());
        log.info("Connection is established.");

        Thread.sleep(500);

        try {
            if (runInBatch) {
                int iter = 1;
                int max = 10;
                do {
                    log.info("Next iteration {} of {}", iter, max);
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        ObservationMessage observationMessage = createTestMessage();
                        distributionClient.publish(observationMessage);
                    }
                    while (distributionClient.getNumberOfMessagesInQueue() > 0) {
                        long queuedCount = distributionClient.getNumberOfMessagesInQueue();
                        long publishedCount = distributionClient.getNumberOfMessagesPublished();
                        log.debug("Messages in queue: {}, published: {}", queuedCount, publishedCount);
                        Thread.sleep(1000);
                    }
                    iter++;

                } while (iter <= max);
            } else {
                ObservationMessage observationMessage = createTestMessage();
                distributionClient.publish(observationMessage);
                Thread.sleep(500);
                try {
                    observationMessage = createTestMessage();
                    distributionClient.publish(observationMessage);
                    Thread.sleep(100);
                    assertFalse(true, "Should have thrown Exception");
                } catch (IotHubConnectionUnstableException e) {
                    log.info("Expected exception: {}", e.getMessage());
                }
            }
        } catch (RealEstateException e) {
            log.error("RealEstateException: {}", e.getMessage(), e);
            long messagesInQueue = distributionClient.getNumberOfMessagesInQueue();
            long messagesPublishec = distributionClient.getNumberOfMessagesPublished();
            long messagesObserved = distributionClient.getNumberOfMessagesObserved();
            boolean isClientHealthy = distributionClient.isHealthy();
            boolean isDistributionThrottled = distributionClient.isThrottled();
            boolean isSendingStopped = distributionClient.isSendingStopped();
            log.info("Messages in queue: {}, published: {}, observed: {}, isClientHealthy: {}, isDistributionThrottled: {}, isSendingStopped: {}",
                    messagesInQueue, messagesPublishec, messagesObserved, isClientHealthy, isDistributionThrottled, isSendingStopped);
        }

        //Closing connection
        log.info("Waiting for replies for {} seconds.", cleanupPeriodSec);
        Thread.sleep(cleanupPeriodSec * 1000);
        log.info("Published a total of: {}", distributionClient.getNumberOfMessagesPublished());
        distributionClient.closeConnection();
        assertFalse(distributionClient.isConnectionEstablished());
        assertFalse(distributionClient.isHealthy(), "The client should have reported unhealthy.");
        log.info("DONE-Connection is closed.");
    }

    protected static AzureObservationDistributionClient createClient(String[] args, Boolean useConfig) {
        AzureObservationDistributionClient distributionClient = null;
        ApplicationProperties config;
        if (useConfig) {
            config = ApplicationProperties.builder()
                    .defaults()
                    .buildAndSetStaticSingleton();

//            deviceClient = new AzureDeviceClient(config.get(SIMULATOR_CONNECTIONSTRING_KEY));
            distributionClient = new AzureObservationDistributionClient(config.get(SIMULATOR_CONNECTIONSTRING_KEY));
        } else {
            if (args.length < 1) {
                log.error("You need to provide \"primary connection string\" from a Device registered in Azure IoTHub.\n" +
                        "Enter this string as the first argument when running this class.");
                System.exit(0);
            }
            String connectionString = args[0];
            log.debug("ConnectionString {}", connectionString);
            distributionClient = new AzureObservationDistributionClient(connectionString);
            //deviceClient = new AzureDeviceClient(connectionString);
        }
        return distributionClient;
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
        String sensorId = "mqtt-error-simulator-sensor-" + random.nextInt(5);
        double temperature = 18.0 + random.nextDouble() * 12; // 18-30°C

        return new ObservationMessageBuilder()
                .withSensorId(sensorId)
                .withRealEstate("StabilityTestRE")
                .withBuilding("TestBuilding")
                .withFloor(String.format("%02d", 1 + random.nextInt(3)))
                .withSection("Section-" + (char) ('A' + random.nextInt(3)))
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