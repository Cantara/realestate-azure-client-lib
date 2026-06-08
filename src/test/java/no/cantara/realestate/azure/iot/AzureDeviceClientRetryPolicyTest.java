package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.transport.NoRetry;
import com.microsoft.azure.sdk.iot.device.transport.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AzureDeviceClientRetryPolicyTest {

    @Test
    void boundedRetryPolicyIsAppliedOnConstruction() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        new AzureDeviceClient(deviceClient);
        // The SDK default retries ~Integer.MAX_VALUE times; we must override it with a bounded policy.
        verify(deviceClient).setRetryPolicy(any(RetryPolicy.class));
    }

    @Test
    void retryPolicyCanBeOverridden() {
        DeviceClient deviceClient = mock(DeviceClient.class);
        AzureDeviceClient client = new AzureDeviceClient(deviceClient);
        NoRetry noRetry = new NoRetry();
        client.setRetryPolicy(noRetry);
        verify(deviceClient).setRetryPolicy(noRetry);
    }
}
