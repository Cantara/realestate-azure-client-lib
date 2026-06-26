package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import org.junit.jupiter.api.Test;

import static com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason.*;
import static com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureDeviceClientConnectionMonitorTest {

    @Test
    void connectedStatusMarksConnectionEstablishedAndStable() {
        AzureDeviceClient client = new AzureDeviceClient(mock(DeviceClient.class));

        client.handleConnectionStatusChange(CONNECTED, CONNECTION_OK, null);

        assertTrue(client.isConnectionEstablished());
        assertFalse(client.isConnectionUnstable());
    }

    @Test
    void retryingMarksUnstableButDoesNotForceClose() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AzureDeviceClient client = new AzureDeviceClient(deviceClient);

        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("blip"));

        assertFalse(client.isConnectionEstablished());
        assertTrue(client.isConnectionUnstable());
        // A blip within the retry budget must not close the client.
        verify(deviceClient, after(300).never()).close();
    }

    @Test
    void sdkGivingUpForceClosesTheClientOffTheCallbackThread() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AzureDeviceClient client = new AzureDeviceClient(deviceClient);

        // The SDK exhausted its reconnect attempts — this is the trigger to break the loop.
        client.handleConnectionStatusChange(DISCONNECTED, RETRY_EXPIRED, new RuntimeException("retries exhausted"));

        // close() runs on a separate daemon thread; await it.
        verify(deviceClient, timeout(2000)).close();
        assertTrue(client.isConnectionUnstable());
        assertFalse(client.isConnectionEstablished());
    }

    @Test
    void stuckRetryingPastBudgetIsForceClosedByTheWatchdog() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        // Short retry budget so the watchdog poll reaches it quickly; the SDK never sends a second
        // status callback, so only the timer can trigger the close.
        AzureDeviceClient client = new AzureDeviceClient(deviceClient, new IotHubConnectionMonitor(200L));

        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("stuck"));

        // No further status change arrives — the watchdog must still force the close once the budget elapses.
        verify(deviceClient, timeout(3000)).close();
        assertTrue(client.isConnectionUnstable());
    }

    @Test
    void cleanCloseIsNotReportedAsUnstable() {
        AzureDeviceClient client = new AzureDeviceClient(mock(DeviceClient.class));

        client.handleConnectionStatusChange(DISCONNECTED, CLIENT_CLOSE, null);

        assertFalse(client.isConnectionUnstable());
    }
}
