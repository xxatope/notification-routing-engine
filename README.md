# notification-routing-engine

Notification routing and delivery service with automatic failover cascade, frequency capping (anti-spam), and in-process atomic idempotency. Built on Scala 3, Cats Effect 3, fs2, and Tapir.

> **Status: learning / demo project.** Storage is in-memory and delivery providers are simulated. See [Known limitations](#known-limitations) before using any part of it in production.

---

## Architecture & Design Decisions

The service follows **Hexagonal / Clean Architecture**:
- **Domain Core**: Pure data types, sealed ADTs (`DeliveryStatus`, `SendResult`, `Priority`), zero external dependencies.
- **Engine Layer**: `RoutingEngine` handles business logic, cascade fallbacks, stale lease recovery, and idempotency guarantees using pure functional effects (`F[_]`).
- **Storage Layer**: Interface-driven (`NotificationRepository`). The default in-memory implementation provides atomic Compare-And-Swap (CAS) via `Ref.modify`.
- **Event-Driven Ingestion**: Optional `KafkaConsumerWorker` powered by `fs2-kafka`, with in-order batch offset commits (`commitBatchWithin`).
- **Security & Data Privacy**:
  - Authenticated endpoints via the `X-API-Key` header, compared in constant time (`MessageDigest.isEqual`) to avoid timing attacks. The service refuses to start without `APP_API_KEY`.
  - The key is held as a Ciris `Secret[String]`. In HTTP logs, `Headers.SensitiveHeaders` (`Authorization`, `Cookie`, `Set-Cookie`) **plus an explicit `X-API-Key` rule** are redacted.
  - API responses strip the message payload (e.g. OTP codes) and device tokens; phone numbers are masked in responses and logs (`+799****67`).

---

## Features

- **Cascade Failover**:
  - Tries fast / low-cost channels first (Push via APNS/FCM).
  - Falls back to Primary SMS on timeout or any push failure.
  - Switches to Backup SMS provider if Primary SMS fails with a transient error.
  - Terminates immediately on a permanent SMS failure (e.g. invalid phone number) without redundant calls.
  - A failed marketing push does not spill into paid SMS. (A marketing request that carries **only** a phone number is still sent via SMS directly.)
- **Traffic Prioritization**:
  - `CRITICAL`: 2FA codes, billing alerts (bypass frequency caps).
  - `MARKETING`: Promos and updates (capped per user sliding window; the slot is refunded if delivery fails).
- **Atomic Idempotency (single process)**:
  - Deduplication via `idempotencyKey`. Parallel requests with the same key inside one running instance trigger delivery once and return the same record. This guarantee does **not** survive a restart (see limitations).
- **Lease Timeout**:
  - A retry that finds a stale `InProgress` record resumes the delivery instead of returning a stuck record forever.
- **Validation**:
  - Required fields, length limits, and "at least one of phone / device token" are checked before any delivery (HTTP 400).
- **Observability**:
  - Kubernetes-style probes: `/health/live` and `/health/ready` (currently static).
  - Type-safe OpenAPI / Swagger UI generated via **Tapir**.

---

## Tech Stack

- **Language:** Scala 3.3 LTS
- **Concurrency & Streaming:** Cats Effect 3, fs2, fs2-kafka
- **HTTP Engine:** Tapir, Http4s Ember, Circe
- **Configuration:** Ciris
- **Logging:** Log4cats, Logback
- **Testing:** MUnit, Cats Effect Testing Suite

---

## Configuration

All settings are environment variables.

| Variable | Default | Description |
|---|---|---|
| `APP_API_KEY` | **required** | Value expected in the `X-API-Key` header. At least 16 characters; the service fails at startup otherwise. |
| `HTTP_HOST` | `0.0.0.0` | Bind address. An invalid value silently falls back to the default. |
| `HTTP_PORT` | `8081` | Bind port. An invalid value silently falls back to the default. |
| `KAFKA_ENABLED` | `false` | Enable the Kafka consumer. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers. |
| `KAFKA_CONSUMER_GROUP` | `notification-engine-group` | Consumer group id. |
| `KAFKA_INPUT_TOPIC` | `notifications.requests` | Input topic. |

---

## Getting Started

### Running locally

`sbt` does **not** read the `.env` file (only `docker compose` does), so export the key in your shell:

```bash
export APP_API_KEY=$(openssl rand -hex 16)
sbt run
```

Or load an existing `.env` into the current shell:

```bash
cp .env.example .env        # then put your key into .env
set -a; source .env; set +a
sbt run
```

The server starts on `http://localhost:8081` (Kafka is disabled by default).
Interactive Swagger UI: **http://localhost:8081/docs**

### Running tests

```bash
sbt test
```

### Running with Docker & Kafka

`docker compose` reads `APP_API_KEY` from `.env` and refuses to start if it is missing:

```bash
cp .env.example .env        # then put your key into .env
docker compose up --build -d
```

* **Application API:** `http://localhost:8081`
* **Swagger Documentation:** `http://localhost:8081/docs`
* **Kafka UI:** `http://localhost:8080`

---

## API Examples

All `/api/v1/...` endpoints require the `X-API-Key` header (requests without a valid key get `401`).
The examples below assume the key is in `$APP_API_KEY`.

### 1. Send Critical Notification (2FA)
```bash
curl -X POST http://localhost:8081/api/v1/notifications \
  -H "X-API-Key: $APP_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "auth-step-40291",
    "userId": "usr_91",
    "priority": "CRITICAL",
    "content": "Your verification code: 9140",
    "phone": "+79991234567",
    "deviceToken": "device-token-abc"
  }'
```
Response (`200 OK` — sensitive content and token are stripped):
```json
{
  "id": "e6a8a18a-a9ad-4bd2-8a9e-880a44abafd3",
  "idempotencyKey": "auth-step-40291",
  "userId": "usr_91",
  "priority": "CRITICAL",
  "maskedPhone": "+799****67",
  "status": {
    "Delivered": {
      "channel": "PUSH",
      "provider": "APNS-FCM-Gateway",
      "deliveredAt": "2026-11-01T20:15:33.729997Z"
    }
  },
  "createdAt": "2026-11-01T20:15:33.485Z",
  "updatedAt": "2026-11-01T20:15:33.731Z"
}
```

### 2. Simulated Cascade Failover
```bash
curl -X POST http://localhost:8081/api/v1/notifications \
  -H "X-API-Key: $APP_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "auth-step-40292",
    "userId": "usr_91",
    "priority": "CRITICAL",
    "content": "Password reset alert",
    "phone": "+79991234567",
    "deviceToken": "fail-transient-push-token"
  }'
```

### 3. Missing key returns 401
```bash
curl -i http://localhost:8081/api/v1/notifications/user/usr_91
```

### 4. Health Probes (no key required)
```bash
curl -i http://localhost:8081/health/live
curl -i http://localhost:8081/health/ready
```

---

## Known limitations

This is a learning / demo project, not a production service.

- **In-memory storage.** All records and frequency counters are lost on restart. Idempotency ("exactly once") holds only within the lifetime of a single process, and memory grows without bound.
- **Single instance.** There is no shared state, so the service cannot be scaled horizontally.
- **Delivery is at-least-once.** After a crash, a message redelivered from Kafka may be sent again. The lease only protects against a stuck delivery inside a live process.
- **Lease takeover is not atomic.** Two simultaneous retries of the same stale request may both resume delivery.
- **Minimal Kafka error handling.** Malformed messages are logged and skipped; an unexpected processing error drops the message. There are no retries and no DLQ.
- **Mock providers only.** Push and SMS gateways are simulated, and the mocks live in `src/main`.
- **Synchronous API.** A request blocks until delivery finishes (up to ~11 s in the worst case), and a fully failed delivery still returns HTTP 200 with a `Failed` status.
- **`Failed` is terminal.** Retrying a failed request requires a new `idempotencyKey`; a rate-limited key stays rate-limited.
- **Single shared API key.** There is no per-client identity or rate limiting.
- **Static health probes.** `/health/ready` does not check dependencies.
- **Lenient host/port config.** Invalid `HTTP_HOST` / `HTTP_PORT` values silently fall back to defaults.

## Roadmap

- PostgreSQL repository (unique index on `idempotency_key`, atomic lease acquisition)
- Retries with backoff and a real DLQ for the Kafka consumer
- Real push/SMS provider adapters and a circuit breaker for the primary SMS gateway
- Dependency-aware readiness probe, metrics, and trace IDs in logs
- Tests for authentication, lease takeover, and configuration validation
