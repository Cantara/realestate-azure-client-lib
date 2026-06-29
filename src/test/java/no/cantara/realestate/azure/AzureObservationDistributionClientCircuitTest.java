package no.cantara.realestate.azure;

import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.azure.iot.MqttSendFailureType;
import no.cantara.realestate.observations.ObservationMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static no.cantara.realestate.azure.AzureObservationDistributionClientTest.buildStubObservation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureObservationDistributionClientCircuitTest {

    AzureDeviceClient azureDeviceClient;
    AzureObservationDistributionClient distributionClient;

    @BeforeEach
    void setUp() {
        azureDeviceClient = mock(AzureDeviceClient.class);
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(true);
        distributionClient = new AzureObservationDistributionClient(azureDeviceClient);
        distributionClient.setHealthy();
    }

    @Test
    void healthyAndSendingUntilOverload() {
        assertTrue(distributionClient.isHealthy());
        assertFalse(distributionClient.isSendingStopped());
    }

    @Test
    void quotaExceededStopsSendingAndMarksUnhealthy() {
        distributionClient.registerFailure(
                new IotHubClientException(IotHubStatusCode.QUOTA_EXCEEDED, "quota gone"),
                buildStubObservation());

        assertTrue(distributionClient.isSendingStopped());
        assertFalse(distributionClient.isHealthy());
        assertEquals(MqttSendFailureType.QUOTA_EXCEEDED, distributionClient.getSendStoppedReason());
        // Quota exhausted -> SDK retries must be disabled too.
        verify(azureDeviceClient).useNoRetryPolicy();
    }

    @Test
    void publishIsRejectedWhileCircuitOpen() {
        distributionClient.registerFailure(
                new IotHubClientException(IotHubStatusCode.QUOTA_EXCEEDED, "quota gone"),
                buildStubObservation());

        ObservationMessage message = buildStubObservation();
        distributionClient.publish(message);

        assertEquals(1, distributionClient.getNumberOfMessagesRejected());
        // Rejected, not handed to the device client.
        verify(azureDeviceClient, never()).sendEventAsync(any(), any());
    }

    @Test
    void singleThrottleDoesNotStopSending() {
        distributionClient.registerFailure(
                new IotHubClientException(IotHubStatusCode.THROTTLED, "slow down"),
                buildStubObservation());

        assertFalse(distributionClient.isSendingStopped());
        assertTrue(distributionClient.isHealthy());
    }
}
