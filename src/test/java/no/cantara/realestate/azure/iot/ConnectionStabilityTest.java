package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.AzureObservationDistributionClient;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.slf4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.slf4j.LoggerFactory.getLogger;


public class ConnectionStabilityTest {
    private static final Logger log = getLogger(ConnectionStabilityTest.class);

    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong connectionsLost = new AtomicLong(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Random random = new Random();

    private AzureObservationDistributionClient distributionClient;
    private ScheduledExecutorService scheduler;

    public static void main(String[] args) {
        try {
            ConnectionStabilityTest client = new ConnectionStabilityTest();
            client.run(args);
        } catch (Exception e) {
            log.error("Test failed", e);
            System.exit(1);
        }
    }

    public void run(String[] args) throws InterruptedException {
        TestConfig config = parseArgs(args);

        if (config.showHelp) {
            printHelp();
            return;
        }

        log.info("=== Azure IoT Hub Connection Stability Test ===");
        log.info("Duration: {} minutes", config.durationMinutes);
        log.info("Message interval: {} seconds", config.messageIntervalSeconds);
        log.info("Protocol: {}", config.protocol);
        log.info("Retry enabled: {}", config.retryEnabled);
        log.info("Keep-alive: {} seconds", config.keepAliveSeconds);
        log.info("Started: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log.info("===============================================");

        // Initialize client
        initializeClient(config);

        // Setup scheduler
        scheduler = Executors.newScheduledThreadPool(2);

        // Statistics reporting every minute
        scheduler.scheduleAtFixedRate(this::reportStatistics, 1, 1, TimeUnit.MINUTES);

        // Message sending
        scheduler.scheduleAtFixedRate(() -> sendTestMessage(config),
                5, config.messageIntervalSeconds, TimeUnit.SECONDS); // Start after 5 seconds

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        // Run for specified duration
        Thread.sleep(config.durationMinutes * 60 * 1000L);

        shutdown();
        printFinalReport();
    }

    private void initializeClient(TestConfig config) {
        try {
            String connectionString = getConnectionString(config);

            log.info("Creating Azure device client...");
            AzureDeviceClient deviceClient = new AzureDeviceClient(
                    connectionString
//                    ,
//                    config.protocol,
//                    config.retryEnabled,
//                    config.keepAliveSeconds
            );

            distributionClient = new AzureObservationDistributionClient(deviceClient);
            log.info("Opening connection...");
            distributionClient.openConnection();

            if (!distributionClient.isConnectionEstablished()) {
                throw new RuntimeException("Failed to establish initial connection");
            }

            log.info("Successfully connected to Azure IoT Hub!");

        } catch (Exception e) {
            log.error("Failed to initialize client", e);
            throw new RuntimeException(e);
        }
    }

    private String getConnectionString(TestConfig config) {
        if (config.connectionString != null && !config.connectionString.isEmpty()) {
            return config.connectionString;
        }

        try {
            ApplicationProperties props = ApplicationProperties.builder()
                    .defaults()
                    .buildAndSetStaticSingleton();
            String connectionString = props.get(AzureObservationDistributionClient.CONNECTIONSTRING_KEY);

            if (connectionString == null || connectionString.isEmpty()) {
                throw new IllegalArgumentException(
                        "No connection string found. Either:\n" +
                                "1. Use --connection-string parameter, or\n" +
                                "2. Set " + AzureObservationDistributionClient.CONNECTIONSTRING_KEY + " in local_override.properties"
                );
            }
            return connectionString;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to load connection string from properties", e);
        }
    }

    private void sendTestMessage(TestConfig config) {
        log.info("Preparing to send test message...");
        if (!isRunning.get()) {
            return;
        }

        try {
            // Check connection status
            boolean wasConnected = distributionClient.isConnectionEstablished();
            if (!wasConnected) {
                log.warn("Connection lost, attempting to reconnect...");
                connectionsLost.incrementAndGet();
                try {
                    distributionClient.openConnection();
                    Thread.sleep(2000); // Wait after reconnection
                } catch (Exception e) {
                    log.error("Reconnection failed", e);
                    return;
                }
            }

            ObservationMessage message = createTestMessage();
            log.info("Sending test message: {}", message);
            distributionClient.publish(message);
            messagesSent.incrementAndGet();

            if (config.verbose || messagesSent.get() % 10 == 0) {
                log.info("Sent message #{} - Sensor: {}", messagesSent.get(), message.getSensorId());
            }

        } catch (Exception e) {
            messagesFailed.incrementAndGet();
            log.error("Failed to send message #{}: {}", messagesFailed.get(), e.getMessage());
            if (config.verbose) {
                log.debug("Send failure details:", e);
            }
        }
    }

    private ObservationMessage createTestMessage() {
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

    private void reportStatistics() {
        long sent = messagesSent.get();
        long failed = messagesFailed.get();
        long lost = connectionsLost.get();
        double successRate = (sent + failed) > 0 ? (double) sent / (sent + failed) * 100 : 0;

        log.info("=== Stats [{}] === Sent: {} | Failed: {} | Connections lost: {} | Success: {:.1f}% ===",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                sent, failed, lost, successRate);

        // Also report queue status
        if (distributionClient != null) {
            log.info("Queue size: {} | Connection: {}",
                    distributionClient.getNumberOfMessagesInQueue(),
                    distributionClient.isConnectionEstablished() ? "OK" : "LOST");
        }
    }

    private void printFinalReport() {
        long sent = messagesSent.get();
        long failed = messagesFailed.get();
        long lost = connectionsLost.get();

        System.out.println("\n" + "=".repeat(50));
        System.out.println("         FINAL TEST REPORT");
        System.out.println("=".repeat(50));
        System.out.println("Test completed: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        System.out.println("Messages sent: " + sent);
        System.out.println("Messages failed: " + failed);
        System.out.println("Connections lost: " + lost);
        System.out.println("Success rate: " + String.format("%.2f%%",
                (sent + failed) > 0 ? (double) sent / (sent + failed) * 100 : 0));
        System.out.println("Average uptime: " + String.format("%.1f%%",
                lost > 0 ? (1.0 - (double) lost / (sent + failed)) * 100 : 100.0));
        System.out.println("=".repeat(50));
    }

    private void shutdown() {
        log.info("Shutting down test...");
        isRunning.set(false);

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }

        if (distributionClient != null) {
            distributionClient.closeConnection();
        }
        log.info("Test shutdown complete");
    }

    private TestConfig parseArgs(String[] args) {
        TestConfig config = new TestConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--duration" -> config.durationMinutes = Integer.parseInt(args[++i]);
                case "--interval" -> config.messageIntervalSeconds = Integer.parseInt(args[++i]);
                case "--protocol" -> config.protocol = "MQTT".equals(args[++i]) ?
                        IotHubClientProtocol.MQTT : IotHubClientProtocol.MQTT_WS;
                case "--retry" -> config.retryEnabled = Boolean.parseBoolean(args[++i]);
                case "--keep-alive" -> config.keepAliveSeconds = Integer.parseInt(args[++i]);
                case "--connection-string" -> config.connectionString = args[++i];
                case "--verbose" -> config.verbose = true;
                case "--help" -> config.showHelp = true;
            }
        }

        return config;
    }

    private void printHelp() {
        System.out.println("Azure IoT Hub Connection Stability Test\n");
        System.out.println("Usage with Maven:");
        System.out.println("  mvn exec:java -Dexec.mainClass=\"no.cantara.realestate.azure.ConnectionStabilityTest\" \\");
        System.out.println("                -Dexec.args=\"--duration 60 --protocol MQTT\"\n");
        System.out.println("Parameters:");
        System.out.println("  --duration <min>      Test duration in minutes (default: 60)");
        System.out.println("  --interval <sec>      Seconds between messages (default: 30)");
        System.out.println("  --protocol <type>     MQTT or MQTT_WS (default: MQTT_WS)");
        System.out.println("  --retry <true/false>  Enable connection retry (default: true)");
        System.out.println("  --keep-alive <sec>    Keep-alive interval (default: 60)");
        System.out.println("  --connection-string   Azure connection string (or use properties)");
        System.out.println("  --verbose             Enable verbose logging");
        System.out.println("  --help               Show this help\n");
        System.out.println("Examples:");
        System.out.println("  # 2 hour test with MQTT and 10 second intervals:");
        System.out.println("  mvn exec:java -Dexec.mainClass=\"no.cantara.realestate.azure.ConnectionStabilityTest\" \\");
        System.out.println("                -Dexec.args=\"--duration 120 --interval 10 --protocol MQTT\"\n");
        System.out.println("  # Verbose test with longer keep-alive:");
        System.out.println("  mvn exec:java -Dexec.mainClass=\"no.cantara.realestate.azure.ConnectionStabilityTest\" \\");
        System.out.println("                -Dexec.args=\"--verbose --keep-alive 120\"");
    }

    private static class TestConfig {
        int durationMinutes = 60;
        int messageIntervalSeconds = 30;
        IotHubClientProtocol protocol = IotHubClientProtocol.MQTT_WS;
        boolean retryEnabled = true;
        int keepAliveSeconds = 30;
        String connectionString = null;
        boolean verbose = false;
        boolean showHelp = false;
    }
}
