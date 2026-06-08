# MQTT Throttling / Overload Prevention — Analysis

Background for issue [#438](https://github.com/Cantara/realestate-azure-client-lib/issues/438)
and its sub-issues [#439](https://github.com/Cantara/realestate-azure-client-lib/issues/439),
[#440](https://github.com/Cantara/realestate-azure-client-lib/issues/440),
[#441](https://github.com/Cantara/realestate-azure-client-lib/issues/441).

## The problem (from #438)

On error, MQTT keeps retrying the same message continuously — up to 23 attempts have been
seen in the logs. When the underlying cause is that we have already sent too many messages
(IoT Hub quota / throttling), the uncontrolled retries turn into a **Denial of Service attack
against ourselves**.

The work is split into:

- **#438** — parent
- **#439** — Detect that a send error relates to MQTT / Azure IoT Hub
- **#440** — Throttle existing sendings (back off on errors)
- **#441** — Stop new sendings when overloaded

## How sending actually works

```
ObservationDistributor (upstream, separate repo)
  -> AzureObservationDistributionClient.publish()
       -> AzureDeviceClient.sendEventAsync()
            -> Azure IoT SDK DeviceClient   (iot-device-client 2.5.0, protocol MQTT_WS)
```

Send is **asynchronous**: `publish()` hands the message to the SDK and returns immediately.
A `MessageSentCallback` fires later with either success or an `IotHubClientException`.

Relevant files:

- `src/main/java/no/cantara/realestate/azure/AzureObservationDistributionClient.java`
- `src/main/java/no/cantara/realestate/azure/iot/AzureDeviceClient.java`

## Where the "23 attempts" come from

**The retries are NOT in this codebase — they are the Azure IoT SDK's default retry policy.**

Confirmed: there is **no `setRetryPolicy(...)` call anywhere** in the repo. The SDK therefore
uses its built-in `ExponentialBackoffWithJitter`, which retries transient send failures
internally and only invokes our callback *after* it gives up. When IoT Hub throttles or
rejects (e.g. daily quota exceeded), the SDK keeps re-sending the same message → the
up-to-23 attempts seen in the logs.

A second amplifier lives in *this* repo: `publish()` throws
`RealEstateException(RETRY_MAY_FIX_ISSUE)` when the connection is not established
(`AzureObservationDistributionClient.java:122-125`), which signals the upstream distributor
to retry *on top of* the SDK retries.

## The core gap — the failure reason is never inspected

In the send callback the status code is captured but **only logged, never branched on**:

```java
// AzureObservationDistributionClient.java:191
log.debug("...status={}", ...,
    iotHubClientException == null ? OK : iotHubClientException.getStatusCode());

// :161-182 — on error it just calls addMessagesFailed();
//            every failure is treated identically
```

So the code cannot distinguish:

- a transient blip,
- `THROTTLED` (slow down), and
- `QUOTA_EXCEEDED` (the "we already sent too many messages" case from the issue).

Note: the sibling `AzureDataExplorerClient` **does** handle this — it catches
`ThrottleException` and waits 10 seconds before retrying
(`AzureDataExplorerClient.java:57-58`). That is the pattern the MQTT path is missing.

## Mapping to the three sub-issues

### #439 — Detect
- **Finding:** failure reason is discarded; all errors collapse into one generic
  `numberOfMessagesFailed` counter.
- **Handle:** branch on `iotHubClientException.getStatusCode()` in the callback
  (`AzureObservationDistributionClient.java:161`, `:174`); classify
  `THROTTLED` / `QUOTA_EXCEEDED` / transient / fatal.

### #440 — Throttle
- **Finding:** the SDK default `ExponentialBackoffWithJitter` is unbounded-ish and invisible
  to the application; there is no app-level rate limit.
- **Handle:** call `deviceClient.setRetryPolicy(...)` with a bounded policy in
  `AzureDeviceClient`; add outbound rate limiting; mirror the ADX wait-on-throttle pattern.

### #441 — Stop
- **Finding:** there is no circuit breaker; `isHealthy()` is hardcoded to `return true`
  (`AzureObservationDistributionClient.java:223-225`); new publishes always proceed.
- **Handle:** on `QUOTA_EXCEEDED` / repeated `THROTTLED`: set the SDK to `NoRetry`, flip a
  real health flag so `publish()` rejects/pauses new sends, and resume after a cooldown.

## Two cross-cutting observations

Both of these work *against* #441 — the system advertises health and asks for retries
precisely when it should be backing off:

1. `isHealthy()` always returns `true` (`AzureObservationDistributionClient.java:223`).
2. `publish()` throws `RETRY_MAY_FIX_ISSUE` on not-connected
   (`AzureObservationDistributionClient.java:124`).

## Suggested implementation order

`#439 (detect)` is the foundation — both `#440 (throttle)` and `#441 (stop)` depend on being
able to tell *why* a send failed before they can react correctly.

---

# #439 Detection — implemented

The detection step is in place. It does **not** change sending behaviour (no back-off, no
stop yet — those are #440/#441). It only *detects, classifies, counts and reports* why a send
failed, and exposes that to the application using this library.

## What was added

- **`MqttSendFailureType`** (`no.cantara.realestate.azure.iot`) — the failure categories:

  | Category         | Meaning                                                     | `isOverload()` | `isRetryable()` |
  |------------------|-------------------------------------------------------------|:--------------:|:---------------:|
  | `NONE`           | send succeeded                                              | no  | no  |
  | `QUOTA_EXCEEDED` | device message quota exhausted — **root cause of the self-DoS**; retrying makes it worse | yes | **no** |
  | `THROTTLED`      | rate-limited / server busy — back off and retry later       | yes | yes |
  | `TRANSIENT`      | temporary error (timeout, IO, internal server error)        | no  | yes |
  | `FATAL`          | permanent error (bad format, unauthorized, too large)       | no  | no  |
  | `UNKNOWN`        | status code missing/unrecognised                            | no  | no  |

- **`MqttSendFailureClassifier`** (`no.cantara.realestate.azure.iot`) — maps the SDK's
  `IotHubStatusCode` to a `MqttSendFailureType`. Pure, static, unit-tested
  (`MqttSendFailureClassifierTest`).

- **`AzureObservationDistributionClient`** now classifies every failed send in the IoT Hub
  callback, logs at a severity matching the category (`QUOTA_EXCEEDED`/`FATAL` → `error`,
  `THROTTLED`/`TRANSIENT` → `warn`), counts failures per category, records the last failure,
  and tags the OpenTelemetry span with `mqtt.failure.type` and `iothub.status_code`.

## How to use it (for client applications)

The library now exposes the detection signals — poll them from your health check, metrics
publisher, or monitoring loop:

```java
AzureObservationDistributionClient client = ...; // your existing instance

// Most recent failure category and when it happened
MqttSendFailureType last = client.getLastFailureType(); // NONE if nothing has failed
Instant lastAt = client.getLastFailureAt();             // null if nothing has failed

// Count for one category
long quotaFailures = client.getNumberOfMessagesFailed(MqttSendFailureType.QUOTA_EXCEEDED);

// Snapshot of all categories (good for a metrics gauge)
Map<MqttSendFailureType, Long> byType = client.getFailureCountsByType();

// Total failures (unchanged, pre-existing)
long totalFailures = client.getNumberOfMessagesFailed();
```

### Reacting to overload (interim guidance until #440/#441 land)

Detection alone does not stop the self-DoS — the library still sends as before. Until the
throttle/stop work is merged, a consuming application can use these signals to protect itself:

```java
MqttSendFailureType last = client.getLastFailureType();
if (last == MqttSendFailureType.QUOTA_EXCEEDED) {
    // Quota is gone. Retrying is pointless and harmful — pause your producer until the
    // IoT Hub quota window resets (daily quota), and alert.
} else if (last.isOverload()) {
    // THROTTLED — slow down your send rate / apply back-off before producing more.
}
```

Notes:

- The counters are cumulative since process start and never reset.
- `getLastFailureType()` / `getLastFailureAt()` are read without locking (`volatile`); the
  count getters return a consistent snapshot.
- A rising `QUOTA_EXCEEDED` or `THROTTLED` count is the signal that IoT Hub is overloaded.
- For the *health* of sending (stopped or not), use `isHealthy()` / `isSendingStopped()` — wired
  by #441 (below). The per-category counters remain the finest-grained overload signal.

---

# #440 Throttle — implemented

The throttle step **brakes** sending when IoT Hub is overloaded; it does **not** fully stop
(that is #441). It addresses both retry layers identified above.

## Layer 1 — bounded SDK retry policy (`AzureDeviceClient`)

The runaway "same message retried ~23 times" came from the SDK's default
`ExponentialBackoffWithJitter`, which retries ~`Integer.MAX_VALUE` times. `AzureDeviceClient`
now installs a **bounded** policy on construction via `deviceClient.setRetryPolicy(...)`:

| Setting            | Default | Constant in `AzureDeviceClient` |
|--------------------|---------|---------------------------------|
| max retries        | 3       | `DEFAULT_MAX_RETRIES`           |
| min backoff (ms)   | 100     | `DEFAULT_MIN_BACKOFF_MILLIS`    |
| max backoff (ms)   | 10 000  | `DEFAULT_MAX_BACKOFF_MILLIS`    |
| backoff delta (ms) | 100     | `DEFAULT_BACKOFF_DELTA_MILLIS`  |

`AzureDeviceClient.setRetryPolicy(RetryPolicy)` is also exposed so #441 can switch to `NoRetry`
on `QUOTA_EXCEEDED`.

## Layer 2 — adaptive app-level back-off (`MqttSendThrottle`)

`MqttSendThrottle` (`no.cantara.realestate.azure.iot`) keeps an exponential back-off window
driven by the #439 classification:

- an **overload** outcome (`THROTTLED` / `QUOTA_EXCEEDED`, i.e. `isOverload()`) escalates the
  window: `base * 2^(consecutive-1)`, capped at max (defaults 1 s base, 60 s cap);
- a **success** (`NONE`) resets it immediately;
- other failures leave the window to expire on its own.

`AzureObservationDistributionClient.publish()` calls `applyBackpressureIfThrottled()` on the
distributor thread before each send; when a window is active it sleeps for the remaining delay,
which naturally brakes the whole pipeline. The throttle is fed from the send callback
(`recordOutcome(NONE)` on success, `recordOutcome(type)` on failure).

## How to use it (for client applications)

```java
AzureObservationDistributionClient client = ...;

client.isThrottled();              // true while braking due to overload
client.getThrottleBackoffMillis(); // remaining brake delay in ms (0 when not throttled)
client.getConsecutiveOverloads();  // overload responses since last success (resets on success)
```

Notes:

- The brake runs on the **distributor thread** — while throttled, observations accumulate in
  the upstream `ObservationsRepository` queue (bounded; see the silent-drop analysis). This is
  intended braking; the hard stop is #441.
- Defaults are constants, not yet externally configurable — tune in code or follow up with
  config keys if ops needs it.
- `isHealthy()` is now made meaningful by #441 (see below).

---

# #441 Stop — implemented

The stop step is the **hard stop** that complements the #440 brake: when IoT Hub overload is
not transient, new sends are rejected outright until sending recovers. `isHealthy()` now
reflects this.

## Circuit breaker (`MqttSendCircuitBreaker`)

`MqttSendCircuitBreaker` (`no.cantara.realestate.azure.iot`) is a time-based breaker driven by
the #439 classification:

| Trigger | Action | Default cooldown |
|---|---|---|
| `QUOTA_EXCEEDED` (once) | open immediately — quota is gone, retrying can't succeed | 5 min probe cadence |
| `THROTTLED` ≥ threshold in a row | open — persistent back-pressure, not a one-off | 30 s |
| success (`NONE`) | close — resume normal sending | — |

Threshold default: **5** consecutive `THROTTLED` (`DEFAULT_THROTTLE_THRESHOLD`).

While **open**, `allowSend()` returns `false`. After the cooldown elapses it allows a single
**probe** send (pushing the gate forward so probes don't flood while awaiting the async ack);
a successful probe closes the circuit, a failed one re-opens it.

## Wiring in `AzureObservationDistributionClient`

- `publish()` checks `circuitBreaker.allowSend()` first; when open it calls `rejectSend(...)` —
  the message is **dropped on purpose**, but counted (`getNumberOfMessagesRejected()`) and
  logged, so the loss is **defined and observable, never silent**. Buffering was rejected
  (OOM risk) and retrying is the very self-DoS being prevented.
- On `QUOTA_EXCEEDED` the SDK retry policy is switched to `NoRetry`
  (`AzureDeviceClient.useNoRetryPolicy()`); on recovery the bounded policy (#440) is restored
  (`useDefaultRetryPolicy()`).
- **`isHealthy()` now returns `false` while the circuit is open.** A short adaptive brake (#440)
  does *not* make the client unhealthy — only a hard stop does.

## How to use it (for client applications)

```java
AzureObservationDistributionClient client = ...;

client.isHealthy();                  // false while sending is stopped (circuit open)
client.isSendingStopped();           // same signal, affirmative naming
client.getSendStoppedReason();       // QUOTA_EXCEEDED / THROTTLED, or null when healthy
client.getNumberOfMessagesRejected();// messages dropped because sending was stopped
```

Notes:

- Rejected messages are **lost by design** during a stop — monitor
  `getNumberOfMessagesRejected()` and `isHealthy()`/`getSendStoppedReason()` and alert on them.
- Recovery is automatic: a probe send after the cooldown closes the circuit on success.
- Cooldowns/threshold are constants, not yet externally configurable.

---

*Environment at time of analysis: `iot-device-client` 2.5.0, protocol `MQTT_WS`, branch `main`.*
