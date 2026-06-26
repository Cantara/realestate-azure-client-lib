package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import com.microsoft.azure.sdk.iot.device.transport.ExponentialBackoffWithJitter;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import com.microsoft.azure.sdk.iot.device.transport.NoRetry;
import com.microsoft.azure.sdk.iot.device.transport.RetryPolicy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // Tracks the real MQTT link state from the SDK's connection-status callback and decides when a
    // silent reconnect loop must be broken by force-closing the client (azure-iot-sdk-java#1805).
    private final IotHubConnectionMonitor connectionMonitor;
    // Re-evaluates the force-close decision on a timer. The SDK fires the status callback only once
    // when it enters DISCONNECTED_RETRYING and then retries silently — so the retry-budget trigger
    // would never be re-checked from the callback alone. This poller is what makes it fire.
    private final ScheduledExecutorService instabilityWatchdog;
    // Guards the force-close so it runs at most once per instability episode.
    private final AtomicBoolean closing = new AtomicBoolean(false);

    private volatile boolean connectionEstablished = false;
    private boolean retryConnection;
    private String iotHubHostname = "";

    public AzureDeviceClient(String connectionString) {
        ClientOptions clientOptions = ClientOptions.builder().keepAliveInterval(30).build();
        deviceClient = new DeviceClient(connectionString, IotHubClientProtocol.MQTT_WS, clientOptions);
        if (deviceClient != null && deviceClient.getConfig() != null) {
            iotHubHostname = deviceClient.getConfig().getIotHubHostname();
        }
        this.connectionMonitor = new IotHubConnectionMonitor();
        applyBoundedRetryPolicy();
        registerConnectionStatusCallback();
        this.instabilityWatchdog = startInstabilityWatchdog();
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    /*
        Intended for testing
         */
    protected AzureDeviceClient(DeviceClient deviceClient) {
        this(deviceClient, new IotHubConnectionMonitor());
    }

    /*
        Intended for testing — inject a connection monitor with a deterministic clock / short budget.
         */
    AzureDeviceClient(DeviceClient deviceClient, IotHubConnectionMonitor connectionMonitor) {
        this.deviceClient = deviceClient;
        if (deviceClient != null && deviceClient.getConfig() != null) {
            iotHubHostname = deviceClient.getConfig().getIotHubHostname();
        }
        this.connectionMonitor = connectionMonitor;
        applyBoundedRetryPolicy();
        registerConnectionStatusCallback();
        this.instabilityWatchdog = startInstabilityWatchdog();
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    /**
     * Start the timer that re-evaluates the force-close decision while the link is stuck retrying.
     * Polls at a quarter of the retry budget (the SDK won't re-notify us during a silent reconnect
     * loop, so we have to check the clock ourselves). The thread is a daemon and a no-op whenever the
     * link is healthy.
     */
    private ScheduledExecutorService startInstabilityWatchdog() {
        long pollMillis = Math.max(50L, connectionMonitor.getMaxRetryingMillis() / 4);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "iothub-instability-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::evaluateForceClose, pollMillis, pollMillis, TimeUnit.MILLISECONDS);
        return executor;
    }

    /**
     * Re-check whether the reconnect loop should be broken, and force-close if so. Called both from
     * the status callback (immediate, for {@code RETRY_EXPIRED}) and periodically from the watchdog
     * (for the retry-budget case the callback never re-evaluates). Package-private for testing.
     */
    void evaluateForceClose() {
        try {
            if (connectionMonitor.shouldForceClose()) {
                forceCloseDueToInstability();
            }
        } catch (Exception e) {
            log.error("IoT Hub instability watchdog evaluation failed", e);
        }
    }

    /**
     * Subscribe to the SDK's connection-status changes. This is the only channel that surfaces a
     * dropped MQTT link and the SDK's internal reconnect attempts — the per-message send callback
     * never sees them (azure-iot-sdk-java#1805).
     */
    private void registerConnectionStatusCallback() {
        if (deviceClient == null) {
            return;
        }
        deviceClient.setConnectionStatusChangeCallback(
                context -> handleConnectionStatusChange(
                        context.getNewStatus(), context.getNewStatusReason(), context.getCause()),
                null);
    }

    /**
     * Handle one connection-status transition: update the link state, mirror it onto
     * {@link #connectionEstablished}, and force-close the client if the monitor decides the reconnect
     * loop is no longer expected to recover.
     *
     * <p>Package-private so the decision wiring can be driven from a unit test without a live SDK.
     */
    void handleConnectionStatusChange(IotHubConnectionStatus newStatus,
                                      com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason newReason,
                                      Throwable newCause) {
        connectionMonitor.recordStatusChange(newStatus, newReason, newCause);
        connectionEstablished = newStatus == IotHubConnectionStatus.CONNECTED;
        if (newStatus == IotHubConnectionStatus.CONNECTED) {
            log.info("IoT Hub connection status: CONNECTED (reason={})", newReason);
        } else {
            if (newStatus == IotHubConnectionStatus.DISCONNECTED_RETRYING) {
                log.debug("IoT Hub connection status: {} (reason={}, cause={})", newStatus, newReason,
                        newCause == null ? "none" : newCause.toString());
            } else {
                log.warn("IoT Hub connection status: {} (reason={}, cause={})", newStatus, newReason,
                        newCause == null ? "none" : newCause.toString());
            }
        }
        // Immediate path: RETRY_EXPIRED (SDK gave up) is a status transition, so it fires here. The
        // retry-budget case has no further transition and is caught by the watchdog poll instead.
        evaluateForceClose();
    }

    /**
     * Force-close the client to stop the SDK's reconnect threads. The SDK will not stop them on its
     * own; {@code close()} is the only lever. It must run off the callback thread — {@code close()}
     * joins on the very SDK threads that invoke the status callback, so calling it inline deadlocks.
     */
    private void forceCloseDueToInstability() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        connectionMonitor.markClosedDueToInstability();
        Thread closer = new Thread(() -> {
            try {
                log.warn("Force-closing IoT Hub client to break the reconnect loop (status={}, reason={}).",
                        connectionMonitor.getStatus(), connectionMonitor.getReason());
                closeConnection();
            } catch (Exception e) {
                log.error("Failed to force-close IoT Hub client after detected instability", e);
            } finally {
                closing.set(false);
            }
        }, "iothub-instability-closer");
        closer.setDaemon(true);
        closer.start();
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

    /**
     * Stop the SDK from retrying at all (issue #441). Used when the device message quota is
     * exhausted ({@code QUOTA_EXCEEDED}): retrying cannot succeed until the quota window resets and
     * only worsens the self-DoS.
     */
    public void useNoRetryPolicy() {
        setRetryPolicy(new NoRetry());
        log.warn("Switched IoT Hub retry policy to NoRetry (quota exhausted / sending stopped).");
    }

    /**
     * Restore the bounded retry policy (issue #440) — used when sending recovers after having been
     * stopped by {@link #useNoRetryPolicy()}.
     */
    public void useDefaultRetryPolicy() {
        applyBoundedRetryPolicy();
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

    /**
     * @return {@code true} when sending should be treated as stopped because the MQTT link is
     * unstable (retrying / disconnected with an error) or the client has been force-closed to break
     * a reconnect loop. A clean, intended close is not reported as unstable.
     */
    public boolean isConnectionUnstable() {
        return connectionMonitor.isConnectionUnstableOrStopped();
    }

    /** @return the last connection status reported by the SDK. */
    public IotHubConnectionStatus getConnectionStatus() {
        return connectionMonitor.getStatus();
    }

    /** @return the reason behind the last connection-status change, or {@code null} if none yet. */
    public com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason getConnectionStatusReason() {
        return connectionMonitor.getReason();
    }

    /** @return how long the link has been retrying to reconnect, in ms, or {@code 0} if not retrying. */
    public long getConnectionRetryingForMillis() {
        return connectionMonitor.getRetryingForMillis();
    }

    public static void main(String[] args) {

    }
}
