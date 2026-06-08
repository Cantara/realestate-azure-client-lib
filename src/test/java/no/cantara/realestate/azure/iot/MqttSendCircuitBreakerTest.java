package no.cantara.realestate.azure.iot;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MqttSendCircuitBreakerTest {

    private final AtomicLong now = new AtomicLong(0);

    private MqttSendCircuitBreaker breaker(long quotaCooldown, int throttleThreshold, long throttleCooldown) {
        return new MqttSendCircuitBreaker(quotaCooldown, throttleThreshold, throttleCooldown, now::get);
    }

    @Test
    void closedAllowsSendByDefault() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        assertTrue(b.allowSend());
        assertFalse(b.isOpen());
        assertNull(b.getOpenReason());
    }

    @Test
    void quotaExceededOpensImmediatelyAndRejects() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED, 1);
        assertTrue(b.isOpen());
        assertEquals(MqttSendFailureType.QUOTA_EXCEEDED, b.getOpenReason());
        assertFalse(b.allowSend(), "must reject while open within cooldown");
    }

    @Test
    void throttleBelowThresholdDoesNotOpen() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.THROTTLED, 4);
        assertFalse(b.isOpen());
        assertTrue(b.allowSend());
    }

    @Test
    void throttleAtThresholdOpens() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.THROTTLED, 5);
        assertTrue(b.isOpen());
        assertEquals(MqttSendFailureType.THROTTLED, b.getOpenReason());
    }

    @Test
    void allowsSingleProbeAfterCooldownThenGatesAgain() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED, 1);
        assertFalse(b.allowSend());

        now.addAndGet(300_000); // cooldown elapsed
        assertTrue(b.allowSend(), "one probe allowed");
        assertFalse(b.allowSend(), "further sends gated until next cooldown");
        assertTrue(b.isOpen(), "still open until a probe succeeds");
    }

    @Test
    void successClosesAndResumes() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED, 1);
        now.addAndGet(300_000);
        assertTrue(b.allowSend()); // probe

        b.recordOutcome(MqttSendFailureType.NONE, 0); // probe succeeded
        assertFalse(b.isOpen());
        assertNull(b.getOpenReason());
        assertTrue(b.allowSend());
    }

    @Test
    void failedProbeReopens() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED, 1);
        now.addAndGet(300_000);
        assertTrue(b.allowSend()); // probe

        b.recordOutcome(MqttSendFailureType.QUOTA_EXCEEDED, 2); // probe failed
        assertTrue(b.isOpen());
        assertFalse(b.allowSend());
    }

    @Test
    void transientAndFatalDoNotOpen() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(MqttSendFailureType.TRANSIENT, 0);
        b.recordOutcome(MqttSendFailureType.FATAL, 0);
        assertFalse(b.isOpen());
    }

    @Test
    void nullOutcomeIgnored() {
        MqttSendCircuitBreaker b = breaker(300_000, 5, 30_000);
        b.recordOutcome(null, 0);
        assertFalse(b.isOpen());
    }
}
