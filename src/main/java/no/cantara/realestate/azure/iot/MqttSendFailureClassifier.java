package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;

/**
 * Detects what kind of failure an Azure IoT Hub (MQTT) send produced.
 *
 * <p>This is the detection step for issue #439 — the foundation that throttling (#440) and
 * stop-sending (#441) build on. It maps the SDK's {@link IotHubStatusCode} to a
 * {@link MqttSendFailureType}, isolating the rest of the code from the full list of IoT Hub
 * status codes and, crucially, separating {@code THROTTLED} (back off) from {@code QUOTA_EXCEEDED}
 * (stop — retrying makes the self-DoS worse).
 */
public final class MqttSendFailureClassifier {

    private MqttSendFailureClassifier() {
    }

    /**
     * Classify the exception handed to a {@code MessageSentCallback}.
     *
     * @param exception the exception reported by the SDK, or {@code null} when the send succeeded
     * @return the matching {@link MqttSendFailureType}; {@link MqttSendFailureType#NONE} when
     * {@code exception} is {@code null}
     */
    public static MqttSendFailureType classify(IotHubClientException exception) {
        if (exception == null) {
            return MqttSendFailureType.NONE;
        }
        return classify(exception.getStatusCode());
    }

    /**
     * Classify a raw IoT Hub status code.
     *
     * @param statusCode the status code, may be {@code null}
     * @return the matching {@link MqttSendFailureType}; {@link MqttSendFailureType#UNKNOWN} for an
     * unrecognised or {@code null} code
     */
    public static MqttSendFailureType classify(IotHubStatusCode statusCode) {
        if (statusCode == null) {
            return MqttSendFailureType.UNKNOWN;
        }
        switch (statusCode) {
            case OK:
                return MqttSendFailureType.NONE;

            // Overload — root cause of the self-DoS in #438.
            case QUOTA_EXCEEDED:
                return MqttSendFailureType.QUOTA_EXCEEDED;
            case THROTTLED:
            case SERVER_BUSY:
                return MqttSendFailureType.THROTTLED;

            // Temporary — safe to retry after a delay.
            case INTERNAL_SERVER_ERROR:
            case IO_ERROR:
            case DEVICE_OPERATION_TIMED_OUT:
            case MESSAGE_EXPIRED:
            case MESSAGE_CANCELLED_ONCLOSE:
                return MqttSendFailureType.TRANSIENT;

            // Permanent — retrying will not help; drop the message.
            case BAD_FORMAT:
            case UNAUTHORIZED:
            case NOT_FOUND:
            case PRECONDITION_FAILED:
            case REQUEST_ENTITY_TOO_LARGE:
            case ERROR:
                return MqttSendFailureType.FATAL;

            default:
                return MqttSendFailureType.UNKNOWN;
        }
    }
}
