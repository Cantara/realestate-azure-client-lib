package no.cantara.realestate.azure.iot;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MqttSendThrottleTest {

    private final AtomicLong now = new AtomicLong(0);

    private MqttSendThrottle throttle(long base, long max) {
        return new MqttSendThrottle(base, max, now::get);
    }

    @Test
    void noThrottleByDefault() {
        MqttSendThrottle t = throttle(1000, 60000);
        assertEquals(0, t.currentBackoffDelayMillis());
        assertFalse(t.isThrottled());
        assertEquals(0, t.getConsecutiveOverloads());
    }

    @Test
    void overloadStartsBackoff() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(MqttSendFailureType.THROTTLED);
        assertTrue(t.isThrottled());
        assertEquals(1000, t.currentBackoffDelayMillis());
        assertEquals(1, t.getConsecutiveOverloads());
    }

    @Test
    void backoffGrowsExponentiallyPerConsecutiveOverload() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(MqttSendFailureType.THROTTLED);      // 1000 * 2^0
        assertEquals(1000, t.currentBackoffDelayMillis());
        t.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED); // 1000 * 2^1
        assertEquals(2000, t.currentBackoffDelayMillis());
        t.recordOutcome(MqttSendFailureType.THROTTLED);      // 1000 * 2^2
        assertEquals(4000, t.currentBackoffDelayMillis());
    }

    @Test
    void backoffIsCappedAtMax() {
        MqttSendThrottle t = throttle(1000, 3000);
        for (int i = 0; i < 10; i++) {
            t.recordOutcome(MqttSendFailureType.THROTTLED);
        }
        assertEquals(3000, t.currentBackoffDelayMillis());
    }

    @Test
    void delayDecaysAsTimePasses() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(MqttSendFailureType.THROTTLED);
        assertEquals(1000, t.currentBackoffDelayMillis());
        now.addAndGet(400);
        assertEquals(600, t.currentBackoffDelayMillis());
        now.addAndGet(600);
        assertEquals(0, t.currentBackoffDelayMillis());
        assertFalse(t.isThrottled());
    }

    @Test
    void successResetsBackoff() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(MqttSendFailureType.THROTTLED);
        t.recordOutcome(MqttSendFailureType.THROTTLED);
        assertTrue(t.isThrottled());
        t.recordOutcome(MqttSendFailureType.NONE);
        assertFalse(t.isThrottled());
        assertEquals(0, t.getConsecutiveOverloads());
    }

    @Test
    void nonOverloadFailuresDoNotEscalateOrReset() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(MqttSendFailureType.THROTTLED); // window = 1000
        t.recordOutcome(MqttSendFailureType.TRANSIENT); // no change
        t.recordOutcome(MqttSendFailureType.FATAL);     // no change
        assertEquals(1, t.getConsecutiveOverloads());
        assertEquals(1000, t.currentBackoffDelayMillis());
    }

    @Test
    void nullOutcomeIsIgnored() {
        MqttSendThrottle t = throttle(1000, 60000);
        t.recordOutcome(null);
        assertFalse(t.isThrottled());
    }
}
