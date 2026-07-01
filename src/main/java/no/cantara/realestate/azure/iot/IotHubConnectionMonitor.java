package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import org.slf4j.Logger;

import java.util.function.LongSupplier;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Tracks the Azure IoT Hub link state from the SDK's connection-status callback and decides when the
 * client should be force-closed to break a silent reconnect loop.
 *
 * <p>Background: when the device message quota is exhausted (or the network drops), IoT Hub closes
 * the MQTT connection and the SDK retries internally on its own threads. Those retries are invisible
 * to the per-message {@code MessageSentCallback}; the failure "just spins" inside the SDK
 * (see <a href="https://github.com/Azure/azure-iot-sdk-java/issues/1805">azure-iot-sdk-java#1805</a>).
 * The connection-status callback is the only channel that surfaces it, and the SDK will not stop the
 * retry threads on its own — the application has to call {@code close()}.
 *
 * <p>This monitor is the decision logic behind that. It is deliberately a small, clock-injectable
 * unit (mirroring {@link MqttSendThrottle} / {@link MqttSendCircuitBreaker}) so the timing rules can
 * be unit-tested without a live SDK. The wiring — registering the callback and actually running
 * {@code close()} on a separate thread — lives in {@link AzureDeviceClient}.
 *
 * <p>A network blip is tolerated: {@code DISCONNECTED_RETRYING} alone does not force a close. The
 * client is closed only when recovery is no longer plausible:
 * <ul>
 *     <li>the SDK itself gave up — {@code DISCONNECTED} with reason {@code RETRY_EXPIRED}; or</li>
 *     <li>the link has been stuck in {@code DISCONNECTED_RETRYING} longer than
 *     {@link #DEFAULT_MAX_RETRYING_MILLIS} (a safety net in case the SDK keeps retrying — the
 *     intended reconnect window is ~4 minutes, see
 *     <a href="https://github.com/Azure/azure-iot-sdk-java/issues/377">azure-iot-sdk-java#377</a>).</li>
 * </ul>
 *
 * <p>Thread-safety: status changes arrive on an SDK callback thread while link-state is read from
 * the distributor thread, so all access is {@code synchronized}.
 */
public class IotHubConnectionMonitor {
    private static final Logger log = getLogger(IotHubConnectionMonitor.class);

    /**
     * Safety net for the "SDK keeps retrying forever" case. Set above the SDK's intended ~4-minute
     * reconnect window (#377) so the SDK's own {@code RETRY_EXPIRED} is the normal trigger and this
     * only fires if that never comes.
     */
    public static final long DEFAULT_MAX_RETRYING_MILLIS = 60_000L; // 1 min

    private final long maxRetryingMillis;
    private final LongSupplier clockMillis;

    private IotHubConnectionStatus status = IotHubConnectionStatus.DISCONNECTED;
    private IotHubConnectionStatusChangeReason reason = null;
    private Throwable cause = null;
    private long retryingSinceMillis = 0L;
    private boolean closedDueToInstability = false;

    public IotHubConnectionMonitor() {
        this(DEFAULT_MAX_RETRYING_MILLIS, System::currentTimeMillis);
    }

    public IotHubConnectionMonitor(long maxRetryingMillis) {
        this(maxRetryingMillis, System::currentTimeMillis);
    }

    // Visible for testing — inject a deterministic clock.
    IotHubConnectionMonitor(long maxRetryingMillis, LongSupplier clockMillis) {
        this.maxRetryingMillis = maxRetryingMillis;
        this.clockMillis = clockMillis;
    }

    /**
     * Record a connection-status transition reported by the SDK.
     *
     * @param newStatus the new link status (ignored if {@code null})
     * @param newReason why the SDK changed to this status
     * @param newCause  the underlying throwable, if any
     */
    public synchronized void recordStatusChange(IotHubConnectionStatus newStatus,
                                                IotHubConnectionStatusChangeReason newReason,
                                                Throwable newCause) {
        if (newStatus == null) {
            return;
        }
        this.status = newStatus;
        this.reason = newReason;
        this.cause = newCause;
        switch (newStatus) {
            case CONNECTED:
                // Recovered: clear the retry clock and any prior force-close state.
                retryingSinceMillis = 0L;
                closedDueToInstability = false;
                break;
            case DISCONNECTED_RETRYING:
                if (retryingSinceMillis == 0L) {
                    retryingSinceMillis = clockMillis.getAsLong();
                }
                break;
            case DISCONNECTED:
                // Keep retryingSinceMillis for diagnostics; the reason tells us whether this is a
                // clean close (CLIENT_CLOSE) or the SDK giving up (RETRY_EXPIRED, NO_NETWORK, ...).
                break;
            default:
                // No other statuses exist today; ignore defensively.
        }
    }

    /**
     * @return {@code true} when the client should be force-closed to stop a reconnect loop that is
     * no longer expected to recover (SDK gave up, or stuck retrying past the safety-net budget).
     * Returns {@code false} once {@link #markClosedDueToInstability()} has been called, so the close
     * is triggered only once.
     */
    public synchronized boolean shouldForceClose() {
        if (closedDueToInstability) {
            return false;
        }
        if (isSelfHealingReason(reason)) {
            // Routine SAS-token renewal: the SDK reconnects and mints a fresh token on its own, so
            // do not force-close — that would break its recovery and require us to reopen.
            return false;
        }
        if (status == IotHubConnectionStatus.DISCONNECTED
                && reason == IotHubConnectionStatusChangeReason.RETRY_EXPIRED) {
            return true;
        }
        if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING && retryingSinceMillis > 0L) {
            long retryingFor = clockMillis.getAsLong() - retryingSinceMillis;
            if (retryingFor >= maxRetryingMillis) {
                log.warn("IoT Hub stuck in DISCONNECTED_RETRYING for {} ms (>= {} ms budget); " +
                        "the SDK is not giving up on its own — forcing a close.", retryingFor, maxRetryingMillis);
                return true;
            }
        }
        return false;
    }

    /**
     * Record observed link activity — a successfully delivered message. Like a {@code CONNECTED}
     * event, this proves the link is alive, so the retry clock is cleared and the accumulated
     * retrying time no longer counts toward a force-close. Has no effect once the client has been
     * force-closed (no sends can succeed then).
     */
    public synchronized void recordSuccessfulSend() {
        if (!closedDueToInstability) {
            retryingSinceMillis = 0L;
        }
    }

    /**
     * Mark that the client has been (or is being) force-closed because of link instability. Keeps
     * {@link #shouldForceClose()} from firing again and keeps the link reported as not-healthy until
     * a successful reconnect produces a {@code CONNECTED} event.
     */
    public synchronized void markClosedDueToInstability() {
        closedDueToInstability = true;
    }

    /**
     * @return {@code true} while the link is down in a way that is not a clean, intended close —
     * i.e. retrying, or disconnected with an error reason. A fresh (never-opened) monitor and a
     * deliberate {@code CLIENT_CLOSE} are <em>not</em> reported as unstable.
     */
    public synchronized boolean isLinkUnstable() {
        if (isSelfHealingReason(reason)) {
            // Routine SAS-token renewal (MQTT renews by dropping/reconnecting, not in place). The SDK
            // recovers on its own with a fresh token, so this is not an outage — do not stop sending.
            return false;
        }
        if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING) {
            return true;
        }
        if (status == IotHubConnectionStatus.DISCONNECTED) {
            return reason != null
                    && reason != IotHubConnectionStatusChangeReason.CLIENT_CLOSE
                    && reason != IotHubConnectionStatusChangeReason.CONNECTION_OK;
        }
        return false;
    }

    /**
     * @return {@code true} for disconnect reasons the SDK recovers from on its own, so the monitor
     * must not treat them as an outage. Today this is only {@code EXPIRED_SAS_TOKEN}: on MQTT the SDK
     * renews the SAS token by reconnecting (a routine ~hourly event for a connection-string client),
     * and retrying it — unlike a quota overload — does no harm.
     */
    private static boolean isSelfHealingReason(IotHubConnectionStatusChangeReason reason) {
        return reason == IotHubConnectionStatusChangeReason.EXPIRED_SAS_TOKEN;
    }

    /**
     * @return {@code true} if sending should be treated as stopped because the link is unstable or
     * the client has been force-closed by this monitor. This is the predicate the distributor uses
     * to reject new sends and report unhealthy.
     */
    public synchronized boolean isConnectionUnstableOrStopped() {
        return closedDueToInstability || isLinkUnstable();
    }

    public synchronized boolean isClosedDueToInstability() {
        return closedDueToInstability;
    }

    /**
     * @return the retry budget in milliseconds — how long the link may stay in
     * {@code DISCONNECTED_RETRYING} before {@link #shouldForceClose()} fires. Used by the caller to
     * pace how often it re-evaluates {@code shouldForceClose()} while retrying.
     */
    public long getMaxRetryingMillis() {
        return maxRetryingMillis;
    }

    public synchronized IotHubConnectionStatus getStatus() {
        return status;
    }

    public synchronized IotHubConnectionStatusChangeReason getReason() {
        return reason;
    }

    public synchronized Throwable getCause() {
        return cause;
    }

    /**
     * @return how long the link has been in {@code DISCONNECTED_RETRYING}, in milliseconds, or
     * {@code 0} if it is not currently retrying.
     */
    public synchronized long getRetryingForMillis() {
        if (retryingSinceMillis == 0L) {
            return 0L;
        }
        return Math.max(0L, clockMillis.getAsLong() - retryingSinceMillis);
    }
}
