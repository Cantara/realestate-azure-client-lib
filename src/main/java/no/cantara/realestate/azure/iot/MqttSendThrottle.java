package no.cantara.realestate.azure.iot;

import java.util.function.LongSupplier;

/**
 * Adaptive, application-level back-off for Azure IoT Hub sending (issue #440).
 *
 * <p>This is the "brake existing sendings" half of overload prevention. It does <strong>not</strong>
 * stop sending (that is #441) — it slows the send rate when IoT Hub signals overload, and lets it
 * recover automatically.
 *
 * <p>It reacts to the {@link MqttSendFailureType} produced by the detection step (#439):
 * <ul>
 *     <li>An {@link MqttSendFailureType#isOverload() overload} response
 *     ({@code THROTTLED} / {@code QUOTA_EXCEEDED}) escalates the back-off window exponentially.</li>
 *     <li>A successful send ({@link MqttSendFailureType#NONE}) resets the back-off immediately.</li>
 *     <li>Other failures leave the window to expire on its own.</li>
 * </ul>
 *
 * <p>Callers poll {@link #currentBackoffDelayMillis()} before sending and brake (e.g. sleep) for
 * the returned duration. The class is thread-safe: {@link #recordOutcome} is typically called from
 * the IoT Hub send callback thread while {@link #currentBackoffDelayMillis()} is read from the
 * distributor thread.
 */
public class MqttSendThrottle {

    /** Back-off after the first overload response; doubles per consecutive overload. */
    public static final long DEFAULT_BASE_BACKOFF_MILLIS = 1_000L;
    /** Upper bound on the back-off window. */
    public static final long DEFAULT_MAX_BACKOFF_MILLIS = 60_000L;

    private final long baseBackoffMillis;
    private final long maxBackoffMillis;
    private final LongSupplier clockMillis;

    private int consecutiveOverloads = 0;
    private volatile long throttleUntilMillis = 0L;

    public MqttSendThrottle() {
        this(DEFAULT_BASE_BACKOFF_MILLIS, DEFAULT_MAX_BACKOFF_MILLIS, System::currentTimeMillis);
    }

    public MqttSendThrottle(long baseBackoffMillis, long maxBackoffMillis) {
        this(baseBackoffMillis, maxBackoffMillis, System::currentTimeMillis);
    }

    // Visible for testing — inject a deterministic clock.
    MqttSendThrottle(long baseBackoffMillis, long maxBackoffMillis, LongSupplier clockMillis) {
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.clockMillis = clockMillis;
    }

    /**
     * Update the back-off window based on the outcome of a send.
     *
     * @param failureType the classified outcome; {@code null} is ignored
     */
    public synchronized void recordOutcome(MqttSendFailureType failureType) {
        if (failureType == null) {
            return;
        }
        if (failureType.isOverload()) {
            consecutiveOverloads++;
            throttleUntilMillis = clockMillis.getAsLong() + computeBackoffMillis(consecutiveOverloads);
        } else if (failureType == MqttSendFailureType.NONE) {
            consecutiveOverloads = 0;
            throttleUntilMillis = 0L;
        }
        // TRANSIENT / FATAL / UNKNOWN: leave any active window to expire naturally.
    }

    // Exponential: base * 2^(consecutive-1), capped at maxBackoffMillis. Guards against overflow.
    long computeBackoffMillis(int consecutive) {
        if (consecutive <= 0) {
            return 0L;
        }
        int shift = Math.min(consecutive - 1, 30);
        long backoff = baseBackoffMillis * (1L << shift);
        if (backoff <= 0 || backoff > maxBackoffMillis) {
            backoff = maxBackoffMillis;
        }
        return backoff;
    }

    /**
     * @return milliseconds the caller should brake before the next send, or {@code 0} if sending
     * may proceed at full rate.
     */
    public synchronized long currentBackoffDelayMillis() {
        return Math.max(0L, throttleUntilMillis - clockMillis.getAsLong());
    }

    public synchronized boolean isThrottled() {
        return currentBackoffDelayMillis() > 0L;
    }

    public synchronized int getConsecutiveOverloads() {
        return consecutiveOverloads;
    }
}
