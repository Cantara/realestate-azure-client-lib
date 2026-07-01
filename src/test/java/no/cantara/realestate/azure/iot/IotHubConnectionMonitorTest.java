package no.cantara.realestate.azure.iot;

import com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason.*;
import static com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus.*;
import static org.junit.jupiter.api.Assertions.*;

class IotHubConnectionMonitorTest {

    private final AtomicLong clock = new AtomicLong(1_000L);
    private final IotHubConnectionMonitor monitor = new IotHubConnectionMonitor(300_000L, clock::get);

    @Test
    void freshMonitorIsStableAndDoesNotClose() {
        // Never opened: DISCONNECTED with no reason is not "unstable" and must not trigger a close.
        assertFalse(monitor.isLinkUnstable());
        assertFalse(monitor.isConnectionUnstableOrStopped());
        assertFalse(monitor.shouldForceClose());
    }

    @Test
    void connectedIsHealthy() {
        monitor.recordStatusChange(CONNECTED, CONNECTION_OK, null);
        assertFalse(monitor.isLinkUnstable());
        assertFalse(monitor.shouldForceClose());
        assertEquals(0L, monitor.getRetryingForMillis());
    }

    @Test
    void retryingIsUnstableButToleratedWithinBudget() {
        monitor.recordStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, new RuntimeException("link lost"));

        assertTrue(monitor.isLinkUnstable());
        assertTrue(monitor.isConnectionUnstableOrStopped());
        // A blip: still within the retry budget, so do not force a close yet.
        clock.addAndGet(60_000L);
        assertFalse(monitor.shouldForceClose());
        assertEquals(60_000L, monitor.getRetryingForMillis());
    }

    @Test
    void retryingPastBudgetForcesClose() {
        monitor.recordStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, null);
        clock.addAndGet(300_000L); // reach the budget
        assertTrue(monitor.shouldForceClose(), "stuck retrying past the budget must force a close");
    }

    @Test
    void retryExpiredForcesCloseImmediately() {
        // The SDK gave up on its own — close now to release the threads and flush the queue.
        monitor.recordStatusChange(DISCONNECTED, RETRY_EXPIRED, new RuntimeException("retries exhausted"));
        assertTrue(monitor.shouldForceClose());
    }

    @Test
    void cleanClientCloseIsNotUnstable() {
        // A deliberate close (our own closeConnection) must not be mistaken for instability.
        monitor.recordStatusChange(DISCONNECTED, CLIENT_CLOSE, null);
        assertFalse(monitor.isLinkUnstable());
        assertFalse(monitor.shouldForceClose());
    }

    @Test
    void forceCloseFiresOnlyOnce() {
        monitor.recordStatusChange(DISCONNECTED, RETRY_EXPIRED, null);
        assertTrue(monitor.shouldForceClose());

        monitor.markClosedDueToInstability();
        assertFalse(monitor.shouldForceClose(), "must not keep asking to close once marked");
        assertTrue(monitor.isClosedDueToInstability());
        assertTrue(monitor.isConnectionUnstableOrStopped());
    }

    @Test
    void recoveryClearsForceCloseState() {
        monitor.recordStatusChange(DISCONNECTED, RETRY_EXPIRED, null);
        monitor.markClosedDueToInstability();
        assertTrue(monitor.isConnectionUnstableOrStopped());

        // Watchdog reopened and the link came back.
        monitor.recordStatusChange(CONNECTED, CONNECTION_OK, null);
        assertFalse(monitor.isClosedDueToInstability());
        assertFalse(monitor.isConnectionUnstableOrStopped());
        assertFalse(monitor.shouldForceClose());
    }

    @Test
    void expiredSasTokenWhileRetryingIsSelfHealing_notUnstableNoForceClose() {
        // MQTT renews the SAS token by dropping/reconnecting; the SDK recovers on its own with a
        // fresh token, so this must NOT be treated as an outage that stops sending or force-closes.
        monitor.recordStatusChange(DISCONNECTED_RETRYING, EXPIRED_SAS_TOKEN, new RuntimeException("token expired"));

        assertFalse(monitor.isLinkUnstable());
        assertFalse(monitor.isConnectionUnstableOrStopped());
        // Even well past the retry budget the token renewal must not force a close.
        clock.addAndGet(300_000L);
        assertFalse(monitor.shouldForceClose());
    }

    @Test
    void expiredSasTokenWhileDisconnectedIsSelfHealing() {
        monitor.recordStatusChange(DISCONNECTED, EXPIRED_SAS_TOKEN, new RuntimeException("token expired"));
        assertFalse(monitor.isLinkUnstable());
        assertFalse(monitor.shouldForceClose());
    }

    @Test
    void retryClockResetsAfterReconnect() {
        monitor.recordStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, null);
        clock.addAndGet(120_000L);
        monitor.recordStatusChange(CONNECTED, CONNECTION_OK, null);
        // A later, separate retry starts its budget fresh — the earlier 120s does not carry over.
        monitor.recordStatusChange(DISCONNECTED_RETRYING, NO_NETWORK, null);
        clock.addAndGet(60_000L);
        assertFalse(monitor.shouldForceClose());
        assertEquals(60_000L, monitor.getRetryingForMillis());
    }
}
