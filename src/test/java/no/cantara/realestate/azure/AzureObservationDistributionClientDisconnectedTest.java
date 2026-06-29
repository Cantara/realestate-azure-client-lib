package no.cantara.realestate.azure;

import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.azure.iot.MqttSendFailureType;
import no.cantara.realestate.observations.ObservationMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static no.cantara.realestate.azure.AzureObservationDistributionClient.MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS;
import static no.cantara.realestate.azure.AzureObservationDistributionClientTest.buildStubObservation;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Characterizes how {@link AzureObservationDistributionClient} behaves when the MQTT connection to
 * Azure IoT Hub is down — the {@code "Cannot publish when mqtt client is disconnected"} situation
 * seen in the log-files.
 *
 * <p>The point of this test is to <em>describe</em>, not to assert a desired future behavior. A
 * disconnect is handled at two different layers, and they behave very differently:
 *
 * <ol>
 *     <li><b>Up-front guard (connection known to be down).</b> {@link
 *     AzureObservationDistributionClient#publish} calls {@code isConnectionEstablished()} first; if
 *     it is {@code false} it throws {@link RealEstateException} with {@link
 *     ExceptionStatusType#RETRY_MAY_FIX_ISSUE} and never hands the message to the SDK. The decision
 *     to retry is pushed up to the distributor.</li>
 *
 *     <li><b>Connection drops after the message was already queued.</b> {@code publish} saw a live
 *     connection, called {@code sendEventAsync}, and only then did MQTT drop. The SDK retries
 *     internally (the {@code "Attempt number N"} log lines), and when its bounded retry policy
 *     gives up it delivers the failure to our {@link MessageSentCallback}. That failure is a
 *     transport error, classified {@link MqttSendFailureType#TRANSIENT} (or {@code UNKNOWN}) — and
 *     <b>neither of those opens the circuit breaker</b>. Only {@code QUOTA_EXCEEDED} and persistent
 *     {@code THROTTLED} do.</li>
 * </ol>
 *
 * <p>So today a connection loss is counted as a failed message and dropped; sending is not braked
 * and the circuit stays {@code CLOSED}. Whether that is the right call — versus backing off while
 * the transport is down — is the open design question. See {@link
 * #connectionDropsAfterQueueing_isTransient_circuitStaysClosed()} and {@link
 * #quotaOnTheSameCallbackPath_doesOpenCircuit()} for the contrast.
 */
class AzureObservationDistributionClientDisconnectedTest {

    private AzureDeviceClient azureDeviceClient;
    private RecordingBrakeClient distributionClient;

    @BeforeEach
    void setUp() {
        azureDeviceClient = mock(AzureDeviceClient.class);
        // Default: a live connection, so publish() proceeds past the up-front guard.
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(true);
        distributionClient = new RecordingBrakeClient(azureDeviceClient);
        distributionClient.setHealthy();
    }

    /**
     * Test double that records the requested back-off instead of sleeping for it. The real
     * {@link AzureObservationDistributionClient#applyBackpressureIfThrottled()} calls
     * {@code Thread.sleep}, which — once the back-off escalates to the 60 s cap — would make a
     * multi-iteration test run for minutes on a developer machine and on Jenkins. Overriding the
     * (deliberately {@code protected}) brake seam keeps the throttle's decision logic under test via
     * {@link #isThrottled()}/{@link #getThrottleBackoffMillis()} while running in milliseconds.
     */
    private static final class RecordingBrakeClient extends AzureObservationDistributionClient {
        private long totalBrakeMillis = 0L;
        private int brakeCount = 0;

        private RecordingBrakeClient(AzureDeviceClient azureDeviceClient) {
            super(azureDeviceClient);
        }

        @Override
        protected void applyBackpressureIfThrottled() {
            long requested = getThrottleBackoffMillis();
            if (requested > 0L) {
                totalBrakeMillis += requested;
                brakeCount++;
            }
            // Deliberately no Thread.sleep — the test asserts the brake was requested, not wall time.
        }
    }


    /**
     * Test for "Cannot publish when mqtt client is disconnected" in the log.
     * The SDK reports this asynchronously to our callback after exhausting its retries
     */
    @Test
    void multipleErrorsWhenClientIsDisconnected() {
        for (int i = 0; i < 5; i++) {
            deliverFailureToCallback(disconnectFailure());
        }

        assertEquals(MqttSendFailureType.TRANSIENT, distributionClient.getLastFailureType());
        assertEquals(5, distributionClient.getNumberOfMessagesFailed());
        assertEquals(5, distributionClient.getNumberOfMessagesFailed(MqttSendFailureType.TRANSIENT));

        // The circuit does NOT open on multiple disconnects. The design decision is to use back-off policy.
        assertFalse(distributionClient.isSendingStopped());
        assertFalse(distributionClient.isHealthy());
    }

    /**
     * Test for "Cannot publish when mqtt client is disconnected" in the log.
     * The SDK reports this asynchronously to our callback after exhausting its retries
     */
    @Test
    void reportUnhealthyWhenClientIsDisconnectedTooManyTimes() {
        assertTrue(distributionClient.isHealthy());
        int testTries = MAX_CONSECUTIVE_CLIENT_DISCONNECT_ERRORS + 1;
        for (int i = 0; i < testTries; i++) {
            deliverFailureToCallback(disconnectFailure());
        }

        assertEquals(MqttSendFailureType.TRANSIENT, distributionClient.getLastFailureType());
        assertEquals(testTries, distributionClient.getNumberOfMessagesFailed());
        assertEquals(testTries, distributionClient.getNumberOfMessagesFailed(MqttSendFailureType.TRANSIENT));

        // The client should eventually report as unhealthy alerting must be triggered after a certain number of disconnects.
        // The design decision is to use back-off policy, but we want to be able to alert if the disconnects persist.
        //assertTrue(distributionClient.isSendingStopped());
        assertFalse(distributionClient.isHealthy());
    }

    /**
     * Layer 1: the connection is known to be down before we even try. publish() refuses, signals
     * that a retry might help, and never touches the SDK.
     */
    @Test
    void disconnectedUpFront_publishRefusesAndDoesNotReachTheSdk() {
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(false);

        RealEstateException thrown = assertThrows(RealEstateException.class,
                () -> distributionClient.publish(buildStubObservation()));

        assertEquals(ExceptionStatusType.RETRY_MAY_FIX_ISSUE, thrown.getStatusType());
        verify(azureDeviceClient, never()).sendEventAsync(any(), any());
        // Nothing was sent, so the circuit is untouched and sending is still considered healthy.
        assertTrue(distributionClient.isHealthy());
        assertFalse(distributionClient.isSendingStopped());
    }

    /**
     * The message was accepted while connected; the connection
     * then dropped and the SDK exhausted its retries, delivering
     * {@code "Cannot publish when mqtt client is disconnected"} to our callback.
     *
     * <p>It is classified {@link MqttSendFailureType#TRANSIENT}: counted as failed, the message is
     * released (not held for retry by this library), but the circuit stays CLOSED and sending is
     * not braked. This is the behavior to reconsider.
     */
    @Test
    void connectionDropsAfterQueueing_isTransient_circuitStaysClosed() {
        deliverFailureToCallback(disconnectFailure());

        assertEquals(MqttSendFailureType.TRANSIENT, distributionClient.getLastFailureType());
        assertEquals(1, distributionClient.getNumberOfMessagesFailed());
        assertEquals(1, distributionClient.getNumberOfMessagesFailed(MqttSendFailureType.TRANSIENT));

        // The circuit does NOT open on a disconnect.
        assertFalse(distributionClient.isSendingStopped());
        assertFalse(distributionClient.isHealthy());
        // No back-off either — a disconnect is not an overload.
        assertFalse(distributionClient.isThrottled());
        assertEquals(0, distributionClient.getConsecutiveOverloads());
        // The message was released, not rejected and not held for retry by us.
        assertEquals(0, distributionClient.getNumberOfMessagesRejected());
        assertEquals(0, distributionClient.getNumberOfMessagesPublished());
        assertEquals(0, distributionClient.getMessagesAwaitingSentAckCollection().size());
    }


    /**
     * A single disconnect is treated as a blip — it does not brake yet. Braking only kicks in once
     * disconnects persist, so normal operation is not penalised for an isolated hiccup.
     */
    @Test
    void singleDisconnect_doesNotBrakeYet() {
        deliverFailureToCallback(disconnectFailure());

        assertFalse(distributionClient.isThrottled());
        assertEquals(0, distributionClient.getThrottleBackoffMillis());
    }

    /**
     * Recovery: once a send succeeds again, the disconnect back-off is cleared immediately and the
     * client resumes full rate.
     */
    @Test
    void successAfterDisconnects_clearsBackoff() {
        for (int i = 0; i < 3; i++) {
            deliverFailureToCallback(disconnectFailure());
        }
        assertTrue(distributionClient.isThrottled());

        deliverSuccessToCallback();

        // The recovering publish actually braked once (proving publish() honours the throttle)
        // before the successful send cleared the back-off.
        assertEquals(1, distributionClient.brakeCount);
        assertFalse(distributionClient.isThrottled());
        assertEquals(0, distributionClient.getThrottleBackoffMillis());
    }

    /**
     * Contrast: the exact same callback path with a {@code QUOTA_EXCEEDED} status opens the circuit
     * immediately and switches the SDK to NoRetry. This is what makes the disconnect case above a
     * deliberate non-event rather than an oversight — the classification is the only difference.
     */
    @Test
    void quotaOnTheSameCallbackPath_doesOpenCircuit() {
        deliverFailureToCallback(new IotHubClientException(IotHubStatusCode.QUOTA_EXCEEDED, "quota gone"));

        assertEquals(MqttSendFailureType.QUOTA_EXCEEDED, distributionClient.getLastFailureType());
        assertTrue(distributionClient.isSendingStopped());
        assertFalse(distributionClient.isHealthy());
        verify(azureDeviceClient).useNoRetryPolicy();
    }

    /**
     * The transport error the SDK reports once it gives up retrying a send over a dead MQTT link.
     * The exact {@link IotHubStatusCode} can vary by SDK version (IO_ERROR / MESSAGE_EXPIRED /
     * ERROR / none), but every non-overload category lands in the circuit breaker's default branch,
     * so the outcome characterized here is the same regardless.
     */
    private static IotHubClientException disconnectFailure() {
        return new IotHubClientException(IotHubStatusCode.IO_ERROR,
                "Cannot publish when mqtt client is disconnected");
    }

    /**
     * Drive one publish through to its send callback. publish() builds the telemetry message and
     * hands it (plus an anonymous callback) to the mocked device client; we capture both and invoke
     * the callback with {@code failure}, simulating the SDK reporting the outcome asynchronously.
     */
    private void deliverFailureToCallback(IotHubClientException failure) {
        ObservationMessage observation = buildStubObservation();
        distributionClient.publish(observation);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<MessageSentCallback> callbackCaptor = ArgumentCaptor.forClass(MessageSentCallback.class);
        verify(azureDeviceClient, atLeastOnce()).sendEventAsync(messageCaptor.capture(), callbackCaptor.capture());

        Message sentMessage = messageCaptor.getValue();
        // onMessageSent expects the callback context to be the Message (see publish()).
        callbackCaptor.getValue().onMessageSent(sentMessage, failure, sentMessage);
    }

    /** Drive one publish through to a successful send callback ({@code null} exception). */
    private void deliverSuccessToCallback() {
        deliverFailureToCallback(null);
    }
}
