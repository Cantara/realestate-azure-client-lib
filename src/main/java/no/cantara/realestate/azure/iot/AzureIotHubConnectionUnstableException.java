package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.RealEstateException;

/**
 * Thrown by {@code AzureObservationDistributionClient.publish(..)} when the Azure IoT Hub link is
 * unstable — the MQTT connection is down and the SDK is either retrying, has given up, or has been
 * force-closed to break a reconnect loop.
 *
 * <p>This is the administrator alarm. The cause cannot be pinned down from the client side: a
 * dropped link looks identical whether the network is down or the device message quota is exhausted
 * (IoT Hub simply closes the connection in both cases). So the message says exactly that — "network
 * down or quota exhausted, cannot determine which" — and carries the SDK's
 * {@link IotHubConnectionStatus} and {@link IotHubConnectionStatusChangeReason} so an operator has
 * the raw signal to investigate.
 *
 * <p>It carries {@link ExceptionStatusType#connection_error} rather than {@code RETRY_MAY_FIX_ISSUE}
 * on purpose: while the link is unstable, having the upstream distributor retry immediately just
 * piles more doomed sends onto a dead connection. New messages are rejected until the link recovers.
 *
 * @example
 * <pre>{@code
 * try {
 *     distributionClient.publish(observation);
 * } catch (AzureIotHubConnectionUnstableException e) {
 *     alerting.raise("IoT Hub link unstable: " + e.getMessage(),
 *                    e.getConnectionStatus(), e.getReason());
 * }
 * }</pre>
 */
public class AzureIotHubConnectionUnstableException extends RealEstateException {

    private final IotHubConnectionStatus connectionStatus;
    private final IotHubConnectionStatusChangeReason reason;
    private final long retryingForMillis;

    public AzureIotHubConnectionUnstableException(IotHubConnectionStatus connectionStatus,
                                                  IotHubConnectionStatusChangeReason reason,
                                                  long retryingForMillis) {
        super(buildMessage(connectionStatus, reason, retryingForMillis), ExceptionStatusType.connection_error);
        this.connectionStatus = connectionStatus;
        this.reason = reason;
        this.retryingForMillis = retryingForMillis;
    }

    private static String buildMessage(IotHubConnectionStatus connectionStatus,
                                       IotHubConnectionStatusChangeReason reason,
                                       long retryingForMillis) {
        return "Azure IoT Hub connection is unstable (status=" + connectionStatus
                + ", reason=" + reason
                + ", retryingForMs=" + retryingForMillis
                + "). The network may be down or the device message quota may be exhausted — the client "
                + "cannot determine which. New messages are being rejected until the link recovers.";
    }

    public IotHubConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    public IotHubConnectionStatusChangeReason getReason() {
        return reason;
    }

    public long getRetryingForMillis() {
        return retryingForMillis;
    }
}
