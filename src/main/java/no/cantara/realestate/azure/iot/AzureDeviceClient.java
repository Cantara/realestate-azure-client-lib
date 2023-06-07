package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.IotHubClientException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureDeviceClient {
    private static final Logger log = getLogger(AzureDeviceClient.class);

    private final DeviceClient deviceClient;

    boolean connectionEstablished = false;
    private boolean retryConnection;

    public AzureDeviceClient(String connectionString) {
        ClientOptions clientOptions = ClientOptions.builder().keepAliveInterval(30).build();
        deviceClient = new DeviceClient(connectionString, IotHubClientProtocol.MQTT, clientOptions);
    }

    /*
        Intended for testing
         */
    protected AzureDeviceClient(DeviceClient deviceClient) {
        this.deviceClient = deviceClient;
    }

    protected void openConnection() {
        try {
            deviceClient.open(retryConnection);
            connectionEstablished = true;
        } catch (IotHubClientException e) {
            //FIXME handle open connection errors.
            connectionEstablished = false;
            throw new RuntimeException(e);
        }
    }




    public void sendEventAsync(Message message, MessageSentCallback messageSentCallback) {
        deviceClient.sendEventAsync(message, messageSentCallback, message);
    }

    public boolean isConnectionEstablished() {
        return connectionEstablished;
    }

    public static void main(String[] args) {

    }
}
