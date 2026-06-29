package no.cantara.realestate.azure.iot;

/**
 * Classification of the outcome of an Azure IoT Hub (MQTT) send attempt.
 *
 * <p>This is the detection step for issue #439. It turns the raw
 * {@link com.microsoft.azure.sdk.iot.device.IotHubStatusCode} returned by the SDK into a small,
 * stable set of categories that the throttling (#440) and stop-sending (#441) logic can react to
 * without having to know every individual IoT Hub status code.
 *
 * <p>The important distinction is between the two <em>overload</em> categories:
 * <ul>
 *     <li>{@link #THROTTLED} — we are sending too fast right now; backing off will recover.</li>
 *     <li>{@link #QUOTA_EXCEEDED} — the device message quota is exhausted; retrying does
 *         <strong>not</strong> help and actively makes the self-inflicted Denial-of-Service worse.</li>
 * </ul>
 */
public enum MqttSendFailureType {

    /** The send succeeded (no exception was reported). */
    NONE,

    /**
     * IoT Hub daily/device message quota is exhausted ({@code QUOTA_EXCEEDED}). Retrying the same
     * message will keep failing and is the root cause of the self-DoS described in issue #438.
     * New sending should be stopped until the quota window resets.
     */
    QUOTA_EXCEEDED,

    /**
     * IoT Hub is rate-limiting us ({@code THROTTLED} / {@code SERVER_BUSY}). Sending is temporarily
     * refused because we are going too fast. Back off and retry later.
     */
    THROTTLED,

    /**
     * A temporary error that is safe to retry after a short delay (network blip, timeout,
     * internal server error, etc.).
     */
    TRANSIENT,

    /**
     * A permanent error where retrying will not help (bad message format, unauthorized,
     * message too large, ...). The message should be dropped, not retried.
     */
    FATAL,

    /**
     * The message was dropped without ever being delivered because the client connection went away
     * underneath it ({@code MESSAGE_CANCELLED_ONCLOSE} when the client closed,
     * {@code MESSAGE_EXPIRED} when the SDK gave up retrying). The message is <strong>gone</strong> —
     * resending the same payload is pointless. Crucially, seeing this <em>while sending is already
     * stopped</em> (link unstable / force-closed) is the confirmation that the network is down or the
     * device quota is exhausted, and is the trigger for alerting an administrator. Distinct from
     * {@link #TRANSIENT} (which invites a retry) and from {@link #FATAL} (a bad message, not a dead link).
     */
    UNDELIVERABLE,

    /** The status code was missing or not recognised by this classifier. Treat conservatively. */
    UNKNOWN;

    /**
     * @return {@code true} if this failure is caused by IoT Hub overload/back-pressure
     * ({@link #THROTTLED} or {@link #QUOTA_EXCEEDED}) — the cases the throttling and stop-sending
     * logic must react to.
     */
    public boolean isOverload() {
        return this == THROTTLED || this == QUOTA_EXCEEDED;
    }

    /**
     * @return {@code true} if retrying the same message could plausibly succeed.
     * {@link #QUOTA_EXCEEDED} and {@link #FATAL} return {@code false}.
     */
    public boolean isRetryable() {
        return this == THROTTLED || this == TRANSIENT;
    }
}
