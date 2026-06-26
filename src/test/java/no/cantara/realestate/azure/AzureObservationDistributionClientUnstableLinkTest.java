package no.cantara.realestate.azure;

import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.azure.iot.IotHubConnectionUnstableException;
import no.cantara.realestate.azure.iot.MqttSendFailureType;
import no.cantara.realestate.observations.ObservationMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason.NO_NETWORK;
import static com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus.DISCONNECTED_RETRYING;
import static no.cantara.realestate.azure.AzureObservationDistributionClientTest.buildStubObservation;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * The "strup ny sending"/alarm behaviour: when the Azure IoT Hub link is unstable (network down or
 * quota exhausted — indistinguishable from the client side, azure-iot-sdk-java#1805), {@code publish}
 * stops sending and raises a typed {@link IotHubConnectionUnstableException}, and the client reports
 * unhealthy so a reopen-watchdog can act.
 */
class AzureObservationDistributionClientUnstableLinkTest {

    private AzureDeviceClient azureDeviceClient;
    private AzureObservationDistributionClient distributionClient;

    @BeforeEach
    void setUp() {
        azureDeviceClient = mock(AzureDeviceClient.class);
        distributionClient = new AzureObservationDistributionClient(azureDeviceClient);
    }

    @Test
    void unstableLink_publishRejectsWithTypedAlarmAndDoesNotReachSdk() {
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(false);
        when(azureDeviceClient.isConnectionUnstable()).thenReturn(true);
        when(azureDeviceClient.getConnectionStatus()).thenReturn(DISCONNECTED_RETRYING);
        when(azureDeviceClient.getConnectionStatusReason()).thenReturn(NO_NETWORK);
        when(azureDeviceClient.getConnectionRetryingForMillis()).thenReturn(5_000L);

        IotHubConnectionUnstableException thrown = assertThrows(IotHubConnectionUnstableException.class,
                () -> distributionClient.publish(buildStubObservation()));

        // Typed alarm carries the raw SDK signal for the administrator.
        assertEquals(DISCONNECTED_RETRYING, thrown.getConnectionStatus());
        assertEquals(NO_NETWORK, thrown.getReason());
        assertEquals(ExceptionStatusType.connection_error, thrown.getStatusType());
        // Sending is stopped: the message never reaches the SDK and is counted as rejected.
        verify(azureDeviceClient, never()).sendEventAsync(any(), any());
        assertEquals(1, distributionClient.getNumberOfMessagesRejected());
        // The link being unstable is surfaced as unhealthy for the reopen-watchdog.
        assertFalse(distributionClient.isHealthy());
        // ...and as a single "sending stopped" signal, regardless of which source stopped it, so an
        // external caller can poll this and reopen the connection without knowing the internals.
        assertTrue(distributionClient.isSendingStopped());
    }

    @Test
    void stableLink_sendingNotStopped() {
        when(azureDeviceClient.isConnectionUnstable()).thenReturn(false);

        assertFalse(distributionClient.isSendingStopped());
    }

    @Test
    void notYetOpened_stillUsesPlainRetryableRefusal() {
        // A not-yet-opened / cleanly-closed client is NOT an unstable link: keep the legacy refusal.
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(false);
        when(azureDeviceClient.isConnectionUnstable()).thenReturn(false);

        no.cantara.realestate.RealEstateException thrown = assertThrows(no.cantara.realestate.RealEstateException.class,
                () -> distributionClient.publish(buildStubObservation()));

        assertFalse(thrown instanceof IotHubConnectionUnstableException);
        assertEquals(ExceptionStatusType.RETRY_MAY_FIX_ISSUE, thrown.getStatusType());
    }

    @Test
    void cancelledOnClose_isUndeliverableNotRetried() {
        // While connected, a send is accepted; the connection then closes underneath it and the SDK
        // reports MESSAGE_CANCELLED_ONCLOSE — the message is gone, classified UNDELIVERABLE.
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(true);

        ObservationMessage observation = buildStubObservation();
        distributionClient.publish(observation);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<MessageSentCallback> callbackCaptor = ArgumentCaptor.forClass(MessageSentCallback.class);
        verify(azureDeviceClient).sendEventAsync(messageCaptor.capture(), callbackCaptor.capture());
        Message sentMessage = messageCaptor.getValue();
        callbackCaptor.getValue().onMessageSent(sentMessage,
                new IotHubClientException(IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE, "cancelled on close"),
                sentMessage);

        assertEquals(MqttSendFailureType.UNDELIVERABLE, distributionClient.getLastFailureType());
        assertEquals(1, distributionClient.getNumberOfMessagesFailed(MqttSendFailureType.UNDELIVERABLE));
        assertFalse(distributionClient.getLastFailureType().isRetryable());
        assertEquals(0, distributionClient.getNumberOfMessagesPublished());
    }
}
