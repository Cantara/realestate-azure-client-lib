package no.cantara.realestate.azure.iot;

import org.slf4j.Logger;

import java.util.function.LongSupplier;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Stops new Azure IoT Hub sends when IoT Hub is overloaded (issue #441).
 *
 * <p>This is the "stop new sendings" half of overload prevention — the hard stop that complements
 * the adaptive brake in {@link MqttSendThrottle} (#440). It is a time-based circuit breaker driven
 * by the #439 classification:
 *
 * <ul>
 *     <li>{@link MqttSendFailureType#QUOTA_EXCEEDED} opens the circuit immediately — the device
 *     message quota is gone, so retrying cannot succeed until the quota window resets.</li>
 *     <li>{@link MqttSendFailureType#THROTTLED} opens the circuit once it has happened
 *     {@code throttleThreshold} times in a row (persistent back-pressure, not a one-off).</li>
 *     <li>A successful send ({@link MqttSendFailureType#NONE}) closes the circuit and resumes
 *     normal sending.</li>
 * </ul>
 *
 * <p>While the circuit is {@link State#OPEN} {@link #allowSend()} returns {@code false} and the
 * caller must reject (and account for) the message. After the cooldown elapses a single probe send
 * is allowed; if it succeeds the circuit closes, otherwise it re-opens. Thread-safe: failures are
 * recorded from the send-callback thread while {@link #allowSend()} is polled from the distributor
 * thread.
 */
public class MqttSendCircuitBreaker {
    private static final Logger log = getLogger(MqttSendCircuitBreaker.class);

    public enum State {CLOSED, OPEN}

    /** Probe cadence while the device message quota is exhausted. */
    public static final long DEFAULT_QUOTA_COOLDOWN_MILLIS = 300_000L; // 5 min
    /** Consecutive THROTTLED responses before the circuit opens. */
    public static final int DEFAULT_THROTTLE_THRESHOLD = 5;
    /** Cooldown after opening due to persistent throttling. */
    public static final long DEFAULT_THROTTLE_COOLDOWN_MILLIS = 30_000L;

    private final long quotaCooldownMillis;
    private final int throttleThreshold;
    private final long throttleCooldownMillis;
    private final LongSupplier clockMillis;

    private volatile State state = State.CLOSED;
    private long openUntilMillis = 0L;
    private long lastCooldownMillis = 0L;
    private volatile MqttSendFailureType openReason = null;

    public MqttSendCircuitBreaker() {
        this(DEFAULT_QUOTA_COOLDOWN_MILLIS, DEFAULT_THROTTLE_THRESHOLD, DEFAULT_THROTTLE_COOLDOWN_MILLIS,
                System::currentTimeMillis);
    }

    public MqttSendCircuitBreaker(long quotaCooldownMillis, int throttleThreshold, long throttleCooldownMillis) {
        this(quotaCooldownMillis, throttleThreshold, throttleCooldownMillis, System::currentTimeMillis);
    }

    // Visible for testing — inject a deterministic clock.
    MqttSendCircuitBreaker(long quotaCooldownMillis, int throttleThreshold, long throttleCooldownMillis,
                           LongSupplier clockMillis) {
        this.quotaCooldownMillis = quotaCooldownMillis;
        this.throttleThreshold = throttleThreshold;
        this.throttleCooldownMillis = throttleCooldownMillis;
        this.clockMillis = clockMillis;
    }

    /**
     * @return {@code true} if a send may proceed; {@code false} if the circuit is open and the
     * message must be rejected. When the cooldown has elapsed this returns {@code true} for a single
     * probe and pushes the gate forward so probes do not flood while awaiting the async ack.
     */
    public synchronized boolean allowSend() {
        if (state == State.CLOSED) {
            return true;
        }
        long now = clockMillis.getAsLong();
        if (now < openUntilMillis) {
            return false;
        }
        openUntilMillis = now + lastCooldownMillis;
        log.info("MQTT send circuit half-open: probing IoT Hub after cooldown (reason={})", openReason);
        return true;
    }

    /**
     * Update circuit state from the outcome of a send.
     *
     * @param failureType          the classified outcome ({@code null} is ignored)
     * @param consecutiveOverloads consecutive overload responses so far (from {@link MqttSendThrottle})
     */
    public synchronized void recordOutcome(MqttSendFailureType failureType, int consecutiveOverloads) {
        if (failureType == null) {
            return;
        }
        switch (failureType) {
            case QUOTA_EXCEEDED:
                open(failureType, quotaCooldownMillis);
                break;
            case THROTTLED:
                if (consecutiveOverloads >= throttleThreshold) {
                    open(failureType, throttleCooldownMillis);
                }
                break;
            case NONE:
                close();
                break;
            default:
                // TRANSIENT / FATAL / UNKNOWN: not an overload — leave circuit state unchanged.
        }
    }

    private void open(MqttSendFailureType reason, long cooldownMillis) {
        boolean wasClosed = state == State.CLOSED;
        state = State.OPEN;
        openReason = reason;
        lastCooldownMillis = cooldownMillis;
        openUntilMillis = clockMillis.getAsLong() + cooldownMillis;
        if (wasClosed) {
            log.warn("MQTT send circuit OPEN: stopping new sends for {} ms (reason={}). " +
                    "New messages will be rejected until IoT Hub recovers.", cooldownMillis, reason);
        }
    }

    private void close() {
        if (state == State.OPEN) {
            log.warn("MQTT send circuit CLOSED: IoT Hub send succeeded, resuming normal sending " +
                    "(previous reason={}).", openReason);
        }
        state = State.CLOSED;
        openReason = null;
        openUntilMillis = 0L;
    }

    public State getState() {
        return state;
    }

    public boolean isOpen() {
        return state == State.OPEN;
    }

    /**
     * @return the failure category that opened the circuit, or {@code null} if closed.
     */
    public MqttSendFailureType getOpenReason() {
        return openReason;
    }
}
