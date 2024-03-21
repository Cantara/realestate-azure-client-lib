package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
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
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
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
