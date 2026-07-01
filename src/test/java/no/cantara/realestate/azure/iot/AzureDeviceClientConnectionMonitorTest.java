package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason.*;
import static com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void repeatedRetryingPastBudgetForcesClose() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AtomicLong clock = new AtomicLong(1_000L);
        // 60s budget with a controllable clock; the SDK re-fires DISCONNECTED_RETRYING on each attempt.
        AzureDeviceClient client = new AzureDeviceClient(deviceClient, new IotHubConnectionMonitor(60_000L, clock::get));

        // First retry: the budget clock starts, nothing closes yet.
        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("blip"));
        verify(deviceClient, after(200).never()).close();

        // Time passes beyond the budget; the next retry callback re-evaluates and breaks the loop.
        clock.addAndGet(60_000L);
        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("still stuck"));

        // close() runs on a separate daemon thread; await it.
        verify(deviceClient, timeout(2000)).close();
        assertTrue(client.isConnectionUnstable());
    }

    @Test
    void successfulSendResetsTheRetryBudget() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AtomicLong clock = new AtomicLong(1_000L);
        AzureDeviceClient client = new AzureDeviceClient(deviceClient, new IotHubConnectionMonitor(60_000L, clock::get));

        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("blip"));
        clock.addAndGet(50_000L);
        // A message got through — the link is alive, so the budget restarts from here.
        client.notifyMessageDelivered();

        // Another retry arrives; only 20s have elapsed since the reset, so no close yet.
        clock.addAndGet(20_000L);
        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("again"));
        verify(deviceClient, after(200).never()).close();
    }

    @Test
    void expiredSasTokenIsSelfHealing_doesNotForceCloseOrStop() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AzureDeviceClient client = new AzureDeviceClient(deviceClient);

        // Routine SAS-token renewal — the SDK reconnects with a fresh token on its own.
        client.handleConnectionStatusChange(DISCONNECTED_RETRYING, EXPIRED_SAS_TOKEN, new RuntimeException("token expired"));

        assertFalse(client.isConnectionUnstable(), "token renewal must not stop sending");
        verify(deviceClient, after(300).never()).close();
    }

    @Test
    void cleanCloseIsNotReportedAsUnstable() {
        AzureDeviceClient client = new AzureDeviceClient(mock(DeviceClient.class));

        client.handleConnectionStatusChange(DISCONNECTED, CLIENT_CLOSE, null);

        assertFalse(client.isConnectionUnstable());
    }
}
