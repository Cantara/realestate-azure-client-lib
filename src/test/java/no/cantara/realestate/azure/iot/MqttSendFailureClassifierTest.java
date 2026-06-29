package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MqttSendFailureClassifierTest {

    @Test
    void nullExceptionIsNone() {
        assertEquals(MqttSendFailureType.NONE, MqttSendFailureClassifier.classify((IotHubClientException) null));
        assertEquals(MqttSendFailureType.NONE, MqttSendFailureClassifier.classify(IotHubStatusCode.OK));
    }

    @Test
    void nullStatusCodeIsUnknown() {
        assertEquals(MqttSendFailureType.UNKNOWN, MqttSendFailureClassifier.classify((IotHubStatusCode) null));
    }

    @Test
    void quotaExceededIsItsOwnCategory() {
        assertEquals(MqttSendFailureType.QUOTA_EXCEEDED, MqttSendFailureClassifier.classify(IotHubStatusCode.QUOTA_EXCEEDED));
    }

    @Test
    void throttledAndServerBusyAreThrottled() {
        assertEquals(MqttSendFailureType.THROTTLED, MqttSendFailureClassifier.classify(IotHubStatusCode.THROTTLED));
        assertEquals(MqttSendFailureType.THROTTLED, MqttSendFailureClassifier.classify(IotHubStatusCode.SERVER_BUSY));
    }

    @Test
    void serverAndIoErrorsAreTransient() {
        assertEquals(MqttSendFailureType.TRANSIENT, MqttSendFailureClassifier.classify(IotHubStatusCode.INTERNAL_SERVER_ERROR));
        assertEquals(MqttSendFailureType.TRANSIENT, MqttSendFailureClassifier.classify(IotHubStatusCode.IO_ERROR));
        assertEquals(MqttSendFailureType.TRANSIENT, MqttSendFailureClassifier.classify(IotHubStatusCode.DEVICE_OPERATION_TIMED_OUT));
    }

    @Test
    void cancelledOnCloseAndExpiredAreUndeliverable() {
        // The message went away with the connection — not a retry candidate. Seeing this while
        // sending is stopped confirms a dead link (network down or quota exhausted).
        assertEquals(MqttSendFailureType.UNDELIVERABLE, MqttSendFailureClassifier.classify(IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE));
        assertEquals(MqttSendFailureType.UNDELIVERABLE, MqttSendFailureClassifier.classify(IotHubStatusCode.MESSAGE_EXPIRED));
    }

    @Test
    void clientErrorsAreFatal() {
        assertEquals(MqttSendFailureType.FATAL, MqttSendFailureClassifier.classify(IotHubStatusCode.BAD_FORMAT));
        assertEquals(MqttSendFailureType.FATAL, MqttSendFailureClassifier.classify(IotHubStatusCode.UNAUTHORIZED));
        assertEquals(MqttSendFailureType.FATAL, MqttSendFailureClassifier.classify(IotHubStatusCode.REQUEST_ENTITY_TOO_LARGE));
    }

    @Test
    void classifiesFromException() {
        IotHubClientException exception = new IotHubClientException(IotHubStatusCode.QUOTA_EXCEEDED, "quota gone");
        assertEquals(MqttSendFailureType.QUOTA_EXCEEDED, MqttSendFailureClassifier.classify(exception));
    }

    @Test
    void overloadFlag() {
        assertTrue(MqttSendFailureType.QUOTA_EXCEEDED.isOverload());
        assertTrue(MqttSendFailureType.THROTTLED.isOverload());
        assertFalse(MqttSendFailureType.TRANSIENT.isOverload());
        assertFalse(MqttSendFailureType.FATAL.isOverload());
        assertFalse(MqttSendFailureType.NONE.isOverload());
    }

    @Test
    void retryableFlag() {
        assertTrue(MqttSendFailureType.THROTTLED.isRetryable());
        assertTrue(MqttSendFailureType.TRANSIENT.isRetryable());
        assertFalse(MqttSendFailureType.QUOTA_EXCEEDED.isRetryable(), "retrying a quota failure makes the self-DoS worse");
        assertFalse(MqttSendFailureType.FATAL.isRetryable());
        assertFalse(MqttSendFailureType.UNDELIVERABLE.isRetryable(), "an undeliverable message is gone — resending the same payload is pointless");
        assertFalse(MqttSendFailureType.UNDELIVERABLE.isOverload());
    }
}
