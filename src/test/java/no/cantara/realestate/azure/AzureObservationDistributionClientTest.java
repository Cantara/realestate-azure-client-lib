package no.cantara.realestate.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageType;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureObservationDistributionClientTest {

    AzureDeviceClient azureDeviceClient;
    AzureObservationDistributionClient distributionClient;

    @BeforeEach
    void setUp() {
        azureDeviceClient = mock(AzureDeviceClient.class);
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(true);
        distributionClient = new AzureObservationDistributionClient(azureDeviceClient);
    }

    @Test
    void buildTelemetryMessageTest() throws JsonProcessingException {
        ObservationMessage observationMessage = buildStubObservation();
        Message telemetryMessage = distributionClient.buildTelemetryMessage(observationMessage);
        assertEquals(MessageType.DEVICE_TELEMETRY, telemetryMessage.getMessageType());
        String messageString = new String(telemetryMessage.getBytes(), StandardCharsets.UTF_8);
        assertTrue(messageString.contains(observationMessage.getBuilding()));
    }

    @Test
    void publishNoConnection() {
        when(azureDeviceClient.isConnectionEstablished()).thenReturn(false);
        ObservationMessage observationMessage = buildStubObservation();
        assertThrows(RealEstateException.class, () -> distributionClient.publish(observationMessage));
    }

    @Test
    void publish() {

        ObservationMessage observationMessage = buildStubObservation();
        assertEquals(0, distributionClient.getMessagesAwaitingSentAckCollection().size());
        assertEquals(0, distributionClient.getNumberOfMessagesObserved());
        distributionClient.publish(observationMessage);
        assertEquals(1, distributionClient.getMessagesAwaitingSentAckCollection().size());
        assertEquals(1, distributionClient.getNumberOfMessagesObserved());
        verify(azureDeviceClient, times(1)).sendEventAsync(any(com.microsoft.azure.sdk.iot.device.Message.class), any());
    }

    @Test
    void messageSent() {
        ObservationMessage observationMessage = buildStubObservation();
        distributionClient.publish(observationMessage);
        assertEquals(1, distributionClient.getMessagesAwaitingSentAckCollection().size());
        assertEquals(1, distributionClient.getNumberOfMessagesObserved());
        Map<String, ObservationMessage> messagesNeedingAck = distributionClient.getMessagesAwaitingSentAck();
        String messageId = messagesNeedingAck.keySet().iterator().next();
        Message sentMessage = new Message();
        sentMessage.setMessageId(messageId);
        distributionClient.messageSent(sentMessage);
        assertEquals(0, distributionClient.getMessagesAwaitingSentAckCollection().size());

    }

    public static ObservationMessage buildStubObservation() {
        Instant observedAt = Instant.now().minusSeconds(10);
        Instant receivedAt = Instant.now();
        ObservationMessage observationMessage = new ObservationMessageBuilder()
                .withSensorId("rec1")
                .withRealEstate("RE1")
                .withBuilding("Building1")
                .withFloor("04")
                .withSection("Section West")
                .withServesRoom("Room1")
                .withPlacementRoom("Room21")
                .withClimateZone("air1")
                .withElectricityZone("light")
                .withSensorType("temp")
                .withMeasurementUnit("C")
                .withValue(22)
                .withObservationTime(observedAt)
                .withReceivedAt(receivedAt)
                .withTfm("TFM12345")
                .build();
        return observationMessage;
    }
}