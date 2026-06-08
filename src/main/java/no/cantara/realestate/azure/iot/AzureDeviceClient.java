package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import com.microsoft.azure.sdk.iot.device.transport.ExponentialBackoffWithJitter;
import com.microsoft.azure.sdk.iot.device.transport.RetryPolicy;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.slf4j.Logger;

import static no.cantara.realestate.azure.metrics.MetricsConfig.INSTRUMENTATION_SCOPE_NAME;
import static org.slf4j.LoggerFactory.getLogger;

public class AzureDeviceClient {
    private static final Logger log = getLogger(AzureDeviceClient.class);

    // Bounded retry policy (issue #440). The SDK default ExponentialBackoffWithJitter retries
    // ~Integer.MAX_VALUE times, which is the source of the "same message retried ~23 times" runaway
    // that DoS-es us against IoT Hub. Cap the attempts so the SDK gives up and hands control back
    // to the application-level throttle/stop logic.
    static final int DEFAULT_MAX_RETRIES = 3;
    static final long DEFAULT_MIN_BACKOFF_MILLIS = 100L;
    static final long DEFAULT_MAX_BACKOFF_MILLIS = 10_000L;
    static final long DEFAULT_BACKOFF_DELTA_MILLIS = 100L;

    private final DeviceClient deviceClient;
    private final Tracer tracer;

    private boolean connectionEstablished = false;
    private boolean retryConnection;
    private String iotHubHostname = "";

    public AzureDeviceClient(String connectionString) {
        ClientOptions clientOptions = ClientOptions.builder().keepAliveInterval(30).build();
        deviceClient = new DeviceClient(connectionString, IotHubClientProtocol.MQTT_WS, clientOptions);
        if (deviceClient != null && deviceClient.getConfig() != null) {
            iotHubHostname = deviceClient.getConfig().getIotHubHostname();
        }
        applyBoundedRetryPolicy();
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    /*
        Intended for testing
         */
    protected AzureDeviceClient(DeviceClient deviceClient) {
        this.deviceClient = deviceClient;
        if (deviceClient != null && deviceClient.getConfig() != null) {
            iotHubHostname = deviceClient.getConfig().getIotHubHostname();
        }
        applyBoundedRetryPolicy();
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    /**
     * Replace the SDK's effectively-infinite default retry policy with a bounded one, so a single
     * message is retried only a handful of times before the SDK gives up (issue #440).
     */
    private void applyBoundedRetryPolicy() {
        if (deviceClient == null) {
            return;
        }
        RetryPolicy boundedPolicy = new ExponentialBackoffWithJitter(
                DEFAULT_MAX_RETRIES,
                DEFAULT_MIN_BACKOFF_MILLIS,
                DEFAULT_MAX_BACKOFF_MILLIS,
                DEFAULT_BACKOFF_DELTA_MILLIS,
                true);
        deviceClient.setRetryPolicy(boundedPolicy);
        log.info("Configured bounded IoT Hub retry policy: maxRetries={}, minBackoffMillis={}, maxBackoffMillis={}",
                DEFAULT_MAX_RETRIES, DEFAULT_MIN_BACKOFF_MILLIS, DEFAULT_MAX_BACKOFF_MILLIS);
    }

    /**
     * Override the SDK retry policy. Used by #441 to switch to {@code NoRetry} when the device
     * message quota is exhausted, and available for tuning/testing.
     */
    public void setRetryPolicy(RetryPolicy retryPolicy) {
        if (deviceClient != null && retryPolicy != null) {
            deviceClient.setRetryPolicy(retryPolicy);
        }
    }

    public void openConnection() {
        try {
            deviceClient.open(retryConnection);
            connectionEstablished = true;
        } catch (IotHubClientException e) {
            //FIXME handle open connection errors.
            connectionEstablished = false;
            throw new RuntimeException(e);
        }
    }

    public void closeConnection() {
        if (deviceClient != null) {
            deviceClient.close();
            connectionEstablished = false;
        }
    }




    public void sendEventAsync(Message message, MessageSentCallback messageSentCallback) {
        // Extract the SpanContext and other elements from the request.
//        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
//                .extract(Context.current(), httpExchange, getter);
//        try (Scope scope = extractedContext.makeCurrent()) {
            // Automatically use the extracted SpanContext as parent.
            Span span = tracer.spanBuilder("sendEventAsync")
                    .setSpanKind(SpanKind.CLIENT)
                    .startSpan();
            try {
                // Add the attributes defined in the Semantic Conventions
                span.setAttribute("destination.address", iotHubHostname); //serverSpan.setAttribute(SemanticAttributes.DESTINATION_ADDRESS, iotHubHostname);
                span.setAttribute(SemanticAttributes.MESSAGE_TYPE, "SENT");

//                span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_NAME, "messages/events"); <-- QueueName
//                serverSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
//                serverSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
//                serverSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/resource");
                // Serve the request

                deviceClient.sendEventAsync(message, messageSentCallback, message);
            } catch (Exception e) {
                    span.setStatus(StatusCode.ERROR, "Something bad happened!");
                    span.recordException(e);
                    log.error("Failed to send message to Azure IoT Hub", e);
                    throw e;

            } finally {
                span.end();
            }
//        } catch (Exception e) {
//            span.setStatus(StatusCode.ERROR, "Something bad happened!");
//            span.recordException(e);
//        }
    }

    public boolean isConnectionEstablished() {
        return connectionEstablished;
    }

    public static void main(String[] args) {

    }
}
