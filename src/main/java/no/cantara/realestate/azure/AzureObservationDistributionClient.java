package no.cantara.realestate.azure;

import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageSentCallback;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import no.cantara.realestate.azure.iot.AzureDeviceClient;
import no.cantara.realestate.distribution.ObservationDistributionClient;
import no.cantara.realestate.observations.ObservationMessage;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK;
import static org.slf4j.LoggerFactory.getLogger;

public class AzureObservationDistributionClient implements ObservationDistributionClient {
    private static final Logger log = getLogger(AzureObservationDistributionClient.class);

    /**
     *   Message message = PnpConvention.createIotHubMessageUtf8(telemetryName, currentTemperature, componentName);
     *         deviceClient.sendEventAsync(message, new MessageSentCallback(), message);
     *         MessageType.DEVICE_TELEMETRY
     *         DeviceClient#sendEventAsync(Message, MessageSentCallback, Object);
     *         See https://github.com/Azure/azure-iot-sdk-java/blob/main/SDK%20v2%20migration%20guide.md
     */
    protected void sendTelemetryMessage() {

    }
    @Override
    public void publish(ObservationMessage observationMessage) {

    }

    @Override
    public List<ObservationMessage> getObservedMessages() {
        return null;
    }

    private static void sendTemperatureTelemetry(AzureDeviceClient deviceClient, String temperature) {
        String telemetryName = "temperature";
        String telemetryPayload = String.format("{\"%s\": %f}", telemetryName, temperature);

        Message message = new Message(telemetryPayload);
        message.setContentEncoding(StandardCharsets.UTF_8.name());
        message.setContentType("application/json");

        deviceClient.sendEventAsync(message, new ObservationMessageSentCallback());
        log.debug("Telemetry: Sent - {\"{}\": {}°C} with message Id {}.", telemetryName, temperature, message.getMessageId());
    }

    private static class ObservationMessageSentCallback implements MessageSentCallback {
        @Override
        public void onMessageSent(Message sentMessage, IotHubClientException exception, Object callbackContext) {
            Message msg = (Message) callbackContext;
            log.debug("Observation - Response from IoT Hub: message Id={}, status={}", msg.getMessageId(), exception == null ? OK : exception.getStatusCode());
        }
    }
}
