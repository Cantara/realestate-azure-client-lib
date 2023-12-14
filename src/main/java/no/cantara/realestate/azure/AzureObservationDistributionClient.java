package no.cantara.realestate.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.service.AutoService;
import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.telemetry.Duration;
import com.microsoft.applicationinsights.telemetry.RemoteDependencyTelemetry;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.MessageType;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.azure.rec.RecObservationMessage;
import no.cantara.realestate.distribution.ObservationDistributionClient;
import no.cantara.realestate.json.RealEstateObjectMapper;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.utils.LimitedArrayList;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK;
import static org.slf4j.LoggerFactory.getLogger;

@AutoService(ObservationDistributionClient.class)
public class AzureObservationDistributionClient implements ObservationDistributionClient {
    private static final Logger log = getLogger(AzureObservationDistributionClient.class);
    private static final int DEFAULT_MAX_SIZE = 1000;
    public static final String CONNECTIONSTRING_KEY = "distribution.azure.connectionString";

    private final AzureDeviceClient azureDeviceClient;
    private final RealEstateObjectMapper objectMapper;
    private List<ObservationMessage> observedMessages = new LimitedArrayList(DEFAULT_MAX_SIZE);
    private Map<String, ObservationMessage> messagesAwaitingSentAck = new HashMap<>();
    private long numberOfMessagesObserved = 0;

    private final Tracer tracer;
    private final TelemetryClient telemetryClient;

    /*
    Intended used for testing.
     */
    protected AzureObservationDistributionClient(AzureDeviceClient azureDeviceClient) {
        tracer = GlobalOpenTelemetry.getTracer("OTEL.AzureMonitor.AzureObservationDistributionClient");
        telemetryClient = new TelemetryClient();
        this.azureDeviceClient = azureDeviceClient;
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

    /**
     *   Message message = PnpConvention.createIotHubMessageUtf8(telemetryName, currentTemperature, componentName);
     *         deviceClient.sendEventAsync(message, new MessageSentCallback(), message);
     *         MessageType.DEVICE_TELEMETRY
     *         DeviceClient#sendEventAsync(Message, MessageSentCallback, Object);
     *         See https://github.com/Azure/azure-iot-sdk-java/blob/main/SDK%20v2%20migration%20guide.md
     */
    protected void sendTelemetryMessage() {

    }
    @Override
    public void publish(ObservationMessage observationMessage) {
        long startTime = System.currentTimeMillis();
        log.info("Tracer: {}", tracer);
        Span parentSpan = tracer.spanBuilder("IotHub").startSpan();
        try (Scope scope = parentSpan.makeCurrent()) {
//        Span span = tracer.spanBuilder("publishObservationmessage").setSpanKind(SpanKind.CLIENT).startSpan();
//        span.setAttribute("destination.address", "testhostTODO");
            telemetryClient.trackEvent("publishObservationmessage");
            if (!isConnectionEstablished()) {
                telemetryClient.trackEvent("error-publish-observationmessage-not-connected");
                throw new RealEstateException("Connection must explicitly be opened before publishing messages.", ExceptionStatusType.RETRY_NOT_POSSIBLE);
            }
            if (observationMessage == null) {
                telemetryClient.trackEvent("trace-publish-observationmessage-null");
                log.trace("Missing observations message, not able to publish");
                return;
            }
            boolean success = false;
            Span childSpan = tracer.spanBuilder("SendTelemetry").setSpanKind(SpanKind.CLIENT).startSpan();
            childSpan.setAttribute("appName", "IoTHubClient");
            childSpan.setAttribute("server.address", "cludconnectorhub-test-west.azure-devices.net");
            childSpan.setAttribute("http.method", "GET");
            childSpan.setAttribute("http.url", "https://cludconnectorhub-test-west.azure-devices.net/");

            try(Scope childScope = childSpan.makeCurrent()) {
//            try (Scope ignored = span.makeCurrent()) {
                Message telemetryMessage = buildTelemetryMessage(observationMessage);
                String messageId = telemetryMessage.getMessageId();
                messagesAwaitingSentAck.put(messageId, observationMessage);
                azureDeviceClient.sendEventAsync(telemetryMessage, new MessageSentCallback() {
                    @Override
                    public void onMessageSent(Message sentMessage, IotHubClientException iotHubClientException, Object callbackContext) {
                        Message msg = (Message) callbackContext;
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        Duration duration = new Duration(elapsedTime);
                        RemoteDependencyTelemetry dependencyTelemetry = new RemoteDependencyTelemetry("IotHub", "SendEventAsync", duration, true);
                        dependencyTelemetry.setTarget("IotHubNorway.messom.no");
                        dependencyTelemetry.setType("PC"); //--> Device_Type=PC, need Client_Type=PC
                        //koble denne målingen til en eller annen "parent"
                        if (iotHubClientException != null) {
                            dependencyTelemetry.setSuccess(false);
                            childSpan.setStatus(StatusCode.ERROR, iotHubClientException.getMessage());
                            childSpan.setAttribute("http.response.status_code", "500");
                            childSpan.addEvent("Failed to send message");
                            childSpan.recordException(iotHubClientException);
                            log.error("Error sending message", iotHubClientException);
                        } else {
                            //telemetryClient.TrackDependency("myDependencyType", "myDependencyCall", "myDependencyData",  startTime, timer.Elapsed, success);
                            log.info("Message sent: {}", sentMessage);
                            childSpan.setStatus(StatusCode.OK, "Message sent");
                            childSpan.setAttribute("http.response.status_code", "200");
                            childSpan.addEvent("Message sent");
                        }
                        log.debug("Observation - Response from IoT Hub: message Id={}, status={}", msg.getMessageId(), iotHubClientException == null ? OK : iotHubClientException.getStatusCode());
                        messageSent(sentMessage);
                    }
                });
                if (numberOfMessagesObserved < Long.MAX_VALUE) {
                    numberOfMessagesObserved++;
                } else {
                    numberOfMessagesObserved = 1;
                }
                success = true;
            } catch (JsonProcessingException e) {
                childSpan.recordException(e);
                childSpan.setStatus(StatusCode.ERROR, e.getMessage());
                childSpan.setAttribute("http.response.status_code", "500");
                childSpan.addEvent("Failed to parse message: " + observationMessage);
                throw new RuntimeException(e);
            } finally {
                childSpan.end();
                RemoteDependencyTelemetry telemetry = new RemoteDependencyTelemetry();
                telemetry.setSuccess(success);
                telemetry.setTimestamp(new Date(startTime));
                telemetryClient.trackDependency(telemetry);
            }
        }

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
        }
    }
}
