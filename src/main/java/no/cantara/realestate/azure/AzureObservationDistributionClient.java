package no.cantara.realestate.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.service.AutoService;
import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.MessageType;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.azure.iot.*;
import no.cantara.realestate.azure.rec.RecObservationMessage;
import no.cantara.realestate.distribution.ObservationDistributionClient;
import no.cantara.realestate.json.RealEstateObjectMapper;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.plugins.distribution.DistributionService;
import no.cantara.realestate.utils.LimitedArrayList;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK;
import static org.slf4j.LoggerFactory.getLogger;

@AutoService(ObservationDistributionClient.class)
public class AzureObservationDistributionClient implements ObservationDistributionClient, DistributionService {
    private static final Logger log = getLogger(AzureObservationDistributionClient.class);
    private static final int DEFAULT_MAX_SIZE = 1000;
    public static final int MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS = 20;
    public static final String CONNECTIONSTRING_KEY = "distribution.azure.connectionString";

    private final AzureDeviceClient azureDeviceClient;
    private final RealEstateObjectMapper objectMapper;
    private List<ObservationMessage> observedMessages = new LimitedArrayList(DEFAULT_MAX_SIZE);
    private Map<String, ObservationMessage> messagesAwaitingSentAck = new HashMap<>();
    private long numberOfMessagesObserved = 0;
    private long numberOfMessagesPublished = 0;
    private long numberOfMessagesFailed = 0;

    // Detection of MQTT/IoT Hub send failures (issue #439). Counts per failure category plus the
    // most recent failure, so throttling (#440) and stop-sending (#441) have something to react to.
    private final Map<MqttSendFailureType, Long> failuresByType = new EnumMap<>(MqttSendFailureType.class);
    private volatile MqttSendFailureType lastFailureType = MqttSendFailureType.NONE;
    private volatile Instant lastFailureAt = null;

    // Adaptive back-off (issue #440). Brakes the send rate when IoT Hub reports overload
    // (THROTTLED/QUOTA_EXCEEDED) and recovers automatically on success.
    private final MqttSendThrottle sendThrottle = new MqttSendThrottle();

    // Circuit breaker (issue #441). Hard-stops new sends when the quota is exhausted or throttling
    // is persistent; rejected messages are counted (not silently lost) and resume on recovery.
    private final MqttSendCircuitBreaker circuitBreaker = new MqttSendCircuitBreaker();
    private long numberOfMessagesRejected = 0;

    private final Tracer tracer;
    private final TelemetryClient telemetryClient;
    private Instant whenLastMessageDistributedAt = null;

    /*
    Intended used for testing.
     */
    public AzureObservationDistributionClient(AzureDeviceClient azureDeviceClient) {
        tracer = GlobalOpenTelemetry.getTracer("OTEL.AzureMonitor.AzureObservationDistributionClient");
        telemetryClient = new TelemetryClient();
        this.azureDeviceClient = azureDeviceClient;
        objectMapper = RealEstateObjectMapper.getInstance();
    }

    /*
    Intended used for test client
     */
    public AzureObservationDistributionClient(AzureDeviceClient azureDeviceClient, String connectionString) {
        tracer = GlobalOpenTelemetry.getTracer("OTEL.AzureMonitor.AzureObservationDistributionClient");
        telemetryClient = new TelemetryClient();
        this.azureDeviceClient = azureDeviceClient != null ? azureDeviceClient : new AzureDeviceClient(connectionString);
        objectMapper = RealEstateObjectMapper.getInstance();
    }

    public AzureObservationDistributionClient() {
        tracer = GlobalOpenTelemetry.getTracer("OTEL.AzureMonitor.AzureObservationDistributionClient");
        telemetryClient = new TelemetryClient();
        String devicePrimaryConnectionString = no.cantara.config.ApplicationProperties.getInstance().get(CONNECTIONSTRING_KEY);
        if (devicePrimaryConnectionString == null || devicePrimaryConnectionString.isEmpty()) {
            throw new RealEstateException("ConnectionString is missing. Please provide " + CONNECTIONSTRING_KEY + "= in local_override.properties.");
        }
        azureDeviceClient = new AzureDeviceClient(devicePrimaryConnectionString);
        objectMapper = RealEstateObjectMapper.getInstance();
    }

    /**
     *
     * @param devicePrimaryConnectionString retreived from Azure Portal, IoT Hub, Devices, Select Device, Primary connection string
     */
    public AzureObservationDistributionClient(String devicePrimaryConnectionString) {
        tracer = GlobalOpenTelemetry.getTracer("OTEL.AzureMonitor.AzureObservationDistributionClient");
//        final Tracer tracer = GlobalOpenTelemetry.getTracer("no.messom.lekerseg");
//
        telemetryClient = new TelemetryClient();
        azureDeviceClient = new AzureDeviceClient(devicePrimaryConnectionString);
        objectMapper = RealEstateObjectMapper.getInstance();
    }

    public String getName() {
        return "AzureObservationDistributionClient";
    }

    @Override
    public void initialize(Properties properties) {
        azureDeviceClient.openConnection();
    }

    /**
     * Message message = PnpConvention.createIotHubMessageUtf8(telemetryName, currentTemperature, componentName);
     * deviceClient.sendEventAsync(message, new MessageSentCallback(), message);
     * MessageType.DEVICE_TELEMETRY
     * DeviceClient#sendEventAsync(Message, MessageSentCallback, Object);
     * See https://github.com/Azure/azure-iot-sdk-java/blob/main/SDK%20v2%20migration%20guide.md
     */
    protected void sendTelemetryMessage() {

    }

    @Override
    public void publish(ObservationMessage observationMessage) throws RealEstateException {
        if (!isConnectionEstablished()) {
            log.warn("Connection not established, message will be queued/dropped");
            telemetryClient.trackEvent("publish-not-connected");
            throw new RealEstateException(
                    "Connection to AzureDeviceClient is not established",
                    ExceptionStatusType.RETRY_MAY_FIX_ISSUE  // ✅ RETRY_POSSIBLE i stedet!
            );
        }

        if (observationMessage == null) {
            log.trace("Missing observations message, not able to publish");
            return;
        }
        if (!circuitBreaker.allowSend()) {
            rejectSend(observationMessage);
            return;
        }

        applyBackpressureIfThrottled();
        telemetryClient.trackEvent("publish-attempt");

        try {
            log.trace("Publishing observationMessage: {}", observationMessage);
            Message telemetryMessage = buildTelemetryMessage(observationMessage);
            log.trace("Built AzureMessage from observationMessage: {}, with body: {}", telemetryMessage, new String(telemetryMessage.getBytes(), StandardCharsets.UTF_8));
            String messageId = telemetryMessage.getMessageId();
            messagesAwaitingSentAck.put(messageId, observationMessage);
            log.trace("Try to send to Azure IoT Hub: {}", observationMessage);
            Span span = tracer.spanBuilder("iot.send").setSpanKind(SpanKind.PRODUCER).startSpan();
            azureDeviceClient.sendEventAsync(telemetryMessage, new MessageSentCallback() {
                @Override
                public void onMessageSent(Message sentMessage, IotHubClientException iotHubClientException, Object callbackContext) {
                    try {
                        if (iotHubClientException != null) {
                            log.trace("Received Error when sening message to to Azure IoT Hub: {}, Exception: {}", sentMessage, iotHubClientException);
                            MqttSendFailureType failureType = registerFailure(iotHubClientException, observationMessage);
                            telemetryClient.trackEvent("publish-failed-" + failureType.name());
                            span.setStatus(StatusCode.ERROR, iotHubClientException.getMessage());
                            span.recordException(iotHubClientException);
                        } else {
                            log.trace("Message is sent to Azure IoT Hub: {}", sentMessage);
                            addMessagesPublished();
                            registerSuccess();
                            span.setStatus(StatusCode.OK);
                        }
                        messageSent(sentMessage);
                    } finally {
                        span.end();
                    }
                }
            });
            addMessagesObserved();

        } catch (JsonProcessingException e) {
            log.debug("Failed to parse message: {}", observationMessage, e);
            throw new RealEstateException("Failed to parse observation message", e, ExceptionStatusType.data_error);
        }

    }


    @Override
    public boolean isInitialized() {
        return true;
    }

    /**
     * Reflects whether sending is operational. Returns {@code false} when either:
     * <ul>
     *     <li>the send circuit is open (#441) — new messages are being rejected because the IoT Hub
     *     device quota is exhausted or throttling is persistent; or</li>
     *     <li>the MQTT link has been down for {@link #MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS}
     *     consecutive sends ({@code "Cannot publish when mqtt client is disconnected"}). Sending is
     *     <em>not</em> stopped in this case — the adaptive brake (#440) keeps retrying — but a
     *     sustained disconnect is surfaced as unhealthy so monitoring can alert on it.</li>
     * </ul>
     * A short adaptive brake on its own does not make the client unhealthy; only a hard stop or a
     * sustained disconnect does. Recovers automatically: a single successful send resets the
     * disconnect counter and the client reports healthy again.
     */
    @Override
    public boolean isHealthy() {
        boolean isConnectionOpen = !circuitBreaker.isOpen();
        int connectionFailures = sendThrottle.getConsecutiveTransientFailures();
        boolean connectionErrorsBelowTreshold = connectionFailures <= MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS;
        boolean isHealthy = isConnectionOpen && connectionErrorsBelowTreshold;
        log.trace("isHealthy: {}. Based on isConectionOpen {} and connectionFailures/maxAlowed {}/{} ",
                isHealthy, isConnectionOpen, connectionFailures, MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS);
        return isHealthy;
    }

    @Override
    public long getNumberOfMessagesPublished() {
        return numberOfMessagesPublished;
    }

    @Override
    public long getNumberOfMessagesFailed() {
        return numberOfMessagesFailed;
    }

    protected Message buildTelemetryMessage(ObservationMessage observationMessage) throws JsonProcessingException {
        RecObservationMessage recObservationMessage = new RecObservationMessage(observationMessage);
        String observationJson = objectMapper.getObjectMapper().writeValueAsString(recObservationMessage);
        log.trace("Publishing RecObservationMessage: {}", observationJson);
        Message telemetryMessage = new Message(observationJson);
        String messageId = UUID.randomUUID().toString();
        telemetryMessage.setMessageId(messageId);
        telemetryMessage.setMessageType(MessageType.DEVICE_TELEMETRY);
        telemetryMessage.setContentEncoding(StandardCharsets.UTF_8.name());
        telemetryMessage.setContentType("application/json");
        return telemetryMessage;
    }

    @Override
    public long getNumberOfMessagesObserved() {
        return numberOfMessagesObserved;
    }

    @Override
    public List<ObservationMessage> getObservedMessages() {
        return observedMessages;
    }

    public void openConnection() {
        if (azureDeviceClient != null) {
            azureDeviceClient.openConnection();
        }
    }

    public boolean isConnectionEstablished() {
        if (azureDeviceClient != null) {
            return azureDeviceClient.isConnectionEstablished();
        } else {
            return false;
        }
    }

    public void closeConnection() {
        if (azureDeviceClient != null) {
            azureDeviceClient.closeConnection();
        }
    }

    protected Map<String, ObservationMessage> getMessagesAwaitingSentAck() {
        return messagesAwaitingSentAck;
    }

    public Collection<ObservationMessage> getMessagesAwaitingSentAckCollection() {
        return messagesAwaitingSentAck.values();
    }

    private static class ObservationMessageSentCallback implements MessageSentCallback {

        private final AzureObservationDistributionClient distributionClient;

        private ObservationMessageSentCallback(AzureObservationDistributionClient distributionClient) {
            this.distributionClient = distributionClient;
        }

        @Override
        public void onMessageSent(Message sentMessage, IotHubClientException exception, Object callbackContext) {
            Message msg = (Message) callbackContext;
            if (exception != null) {
                log.warn("****");
            }
            log.debug("Observation - Response from IoT Hub: message Id={}, status={}", msg.getMessageId(), exception == null ? OK : exception.getStatusCode());
            distributionClient.messageSent(sentMessage);
        }
    }

    synchronized void messageSent(Message sentMessage) {
        if (sentMessage != null) {
            String messageId = sentMessage.getMessageId();
            ObservationMessage observationMessage = messagesAwaitingSentAck.get(messageId);
            messagesAwaitingSentAck.remove(messageId);
            observedMessages.add(observationMessage);
            updateWhenLastObservationDistributed();
        }
    }

    synchronized void addMessagesObserved() {
        if (numberOfMessagesObserved < Long.MAX_VALUE) {
            numberOfMessagesObserved++;
        } else {
            numberOfMessagesObserved = 1;
        }
    }

    synchronized void addMessagesPublished() {
        if (numberOfMessagesPublished < Long.MAX_VALUE) {
            numberOfMessagesPublished++;
        } else {
            numberOfMessagesPublished = 1;
        }
    }

    synchronized void addMessagesFailed() {
        if (numberOfMessagesFailed < Long.MAX_VALUE) {
            numberOfMessagesFailed++;
        } else {
            numberOfMessagesFailed = 1;
        }
    }

    /**
     * Detect and record what kind of IoT Hub send failure occurred (issue #439).
     * Classifies the exception, updates the per-category counters and the last-failure state, and
     * logs at a severity that matches the category. Returns the detected type so the caller can
     * annotate telemetry/spans.
     *
     * <p>Detection (#439) feeds the adaptive back-off (#440): the classified type is handed to
     * {@link #sendThrottle}, which escalates the brake on overload responses. Stopping new sends
     * outright (#441) is still left out.
     */
    synchronized MqttSendFailureType registerFailure(IotHubClientException exception, ObservationMessage observationMessage) {
        MqttSendFailureType failureType = MqttSendFailureClassifier.classify(exception);
        failuresByType.merge(failureType, 1L, Long::sum);
        lastFailureType = failureType;
        lastFailureAt = Instant.ofEpochMilli(System.currentTimeMillis());
        addMessagesFailed();
        sendThrottle.recordOutcome(failureType);
        circuitBreaker.recordOutcome(failureType, sendThrottle.getConsecutiveOverloads());
        if (failureType == MqttSendFailureType.QUOTA_EXCEEDED && azureDeviceClient != null) {
            // Quota is gone — stop the SDK from retrying too, until sending recovers.
            azureDeviceClient.useNoRetryPolicy();
        }
        IotHubStatusCode statusCode = exception == null ? null : exception.getStatusCode();
        switch (failureType) {
            case QUOTA_EXCEEDED:
                log.error("Azure IoT Hub QUOTA_EXCEEDED: device message quota is exhausted. " +
                        "Retrying makes the self-DoS worse; new sending should stop until the quota window resets. " +
                        "statusCode={}, observationMessage={}", statusCode, observationMessage, exception);
                break;
            case THROTTLED:
                log.warn("Azure IoT Hub THROTTLED/SERVER_BUSY: sending too fast. Back off before retrying. " +
                        "statusCode={}, observationMessage={}", statusCode, observationMessage, exception);
                break;
            case FATAL:
                log.error("Azure IoT Hub permanent send failure: retrying will not help, message should be dropped. " +
                        "statusCode={}, observationMessage={}", statusCode, observationMessage, exception);
                break;
            case TRANSIENT:
                log.warn("Azure IoT Hub transient send failure: safe to retry after a delay. " +
                        "statusCode={}, observationMessage={}", statusCode, observationMessage, exception);
                break;
            default:
                log.warn("Azure IoT Hub unrecognized send failure. statusCode={}, observationMessage={}",
                        statusCode, observationMessage, exception);
        }
        return failureType;
    }

    /**
     * @return how many sends failed with the given {@link MqttSendFailureType} since startup.
     */
    public synchronized long getNumberOfMessagesFailed(MqttSendFailureType failureType) {
        return failuresByType.getOrDefault(failureType, 0L);
    }

    /**
     * @return a snapshot of the failure counts per category since startup. Useful for health
     * endpoints and metrics — e.g. a rising {@link MqttSendFailureType#QUOTA_EXCEEDED} or
     * {@link MqttSendFailureType#THROTTLED} count indicates IoT Hub overload.
     */
    public synchronized Map<MqttSendFailureType, Long> getFailureCountsByType() {
        return new EnumMap<>(failuresByType);
    }

    /**
     * @return the category of the most recent send failure, or {@link MqttSendFailureType#NONE}
     * if no send has failed yet.
     */
    public MqttSendFailureType getLastFailureType() {
        return lastFailureType;
    }

    /**
     * @return the instant of the most recent send failure, or {@code null} if none has occurred.
     */
    public Instant getLastFailureAt() {
        return lastFailureAt;
    }

    /**
     * Brake the send rate when IoT Hub is signalling overload (issue #440). Called at the start of
     * {@link #publish(ObservationMessage)} on the distributor thread, so braking here naturally
     * slows the whole pipeline. When not throttled the delay is {@code 0} and this is a no-op.
     */
    protected void applyBackpressureIfThrottled() {
        long delayMillis = sendThrottle.currentBackoffDelayMillis();
        if (delayMillis <= 0) {
            return;
        }
        log.warn("Braking Azure IoT Hub sending for {} ms after {} consecutive overload responses",
                delayMillis, sendThrottle.getConsecutiveOverloads());
        telemetryClient.trackEvent("throttle-backpressure-applied");
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @return milliseconds the client is currently braking before the next send ({@code 0} when not
     * throttled). A non-zero value means IoT Hub recently reported overload.
     */
    public long getThrottleBackoffMillis() {
        return sendThrottle.currentBackoffDelayMillis();
    }

    /**
     * @return {@code true} if sending is currently being braked due to IoT Hub overload (#440).
     */
    public boolean isThrottled() {
        return sendThrottle.isThrottled();
    }

    /**
     * @return the number of consecutive overload responses (THROTTLED/QUOTA_EXCEEDED) since the
     * last successful send. Resets to 0 on success.
     */
    public int getConsecutiveOverloads() {
        return sendThrottle.getConsecutiveOverloads();
    }

    /**
     * Reject a message because the send circuit is open (issue #441). The message is dropped on
     * purpose — buffering it risks running out of memory and retrying it is the self-DoS we are
     * preventing — but it is counted and logged so the loss is defined and observable, never silent.
     */
    private void rejectSend(ObservationMessage observationMessage) {
        addMessagesRejected();
        telemetryClient.trackEvent("error-publish-observationmessage-circuit-open");
        log.debug("MQTT send circuit OPEN (reason={}); rejecting observationMessage. Rejected total={}",
                circuitBreaker.getOpenReason(), numberOfMessagesRejected);
    }

    /**
     * Record a successful send: reset the back-off (#440), close the circuit if it was open (#441),
     * and restore the bounded retry policy if it had been switched to NoRetry on quota exhaustion.
     */
    private void registerSuccess() {
        sendThrottle.recordOutcome(MqttSendFailureType.NONE);
        boolean wasOpen = circuitBreaker.isOpen();
        circuitBreaker.recordOutcome(MqttSendFailureType.NONE, 0);
        if (wasOpen && !circuitBreaker.isOpen() && azureDeviceClient != null) {
            azureDeviceClient.useDefaultRetryPolicy();
        }
    }

    synchronized void addMessagesRejected() {
        if (numberOfMessagesRejected < Long.MAX_VALUE) {
            numberOfMessagesRejected++;
        } else {
            numberOfMessagesRejected = 1;
        }
    }

    /**
     * @return how many messages were rejected (dropped) because the send circuit was open (#441).
     * A non-zero, rising value means sending is stopped and observations are being lost.
     */
    public long getNumberOfMessagesRejected() {
        return numberOfMessagesRejected;
    }

    /**
     * @return {@code true} if new sends are currently being rejected because IoT Hub is overloaded
     * (the send circuit is open). Mirrors {@code !isHealthy()}.
     */
    public boolean isSendingStopped() {
        return circuitBreaker.isOpen();
    }

    /**
     * @return the failure category that stopped sending ({@code QUOTA_EXCEEDED} or {@code THROTTLED}),
     * or {@code null} if sending is not stopped.
     */
    public MqttSendFailureType getSendStoppedReason() {
        return circuitBreaker.getOpenReason();
    }

    @Override
    public long getNumberOfMessagesInQueue() {
        return messagesAwaitingSentAck.size();
    }

    protected synchronized void updateWhenLastObservationDistributed() {
        whenLastMessageDistributedAt = Instant.ofEpochMilli(System.currentTimeMillis());
    }

    @Override
    public Instant getWhenLastMessageDistributed() {
        return whenLastMessageDistributedAt;
    }
}
