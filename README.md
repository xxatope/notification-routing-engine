# notification-routing-engine

Notification routing and delivery service with automatic failover cascade, frequency capping (anti-spam), and atomic idempotency. Built on Scala 3, Cats Effect 3, fs2, and Tapir.

---

## Architecture & Design Decisions

The service follows **Hexagonal / Clean Architecture**:
- **Domain Core**: Pure data types, sealed ADTs (`DeliveryStatus`, `SendResult`, `Priority`), zero external dependencies.
- **Engine Layer**: `RoutingEngine` handles business logic, cascade fallbacks, stale lease recovery, and idempotency guarantees using pure functional effects (`F[_]`).
- **Storage Layer**: Interface-driven (`NotificationRepository`). Default in-memory implementation provides atomic Compare-And-Swap (CAS) via `Ref.modify`.
- **Event-Driven Ingestion**: Optional `KafkaConsumerWorker` powered by `fs2-kafka` for reactive event streaming with batch commit (`commitBatchWithin`).
- **Security & Data Privacy**: 
  - Authenticated endpoints via `X-API-Key` header with constant-time equality check (`MessageDigest.isEqual`) to prevent timing attacks.
  - Secret credentials masked in logs via Ciris `Secret[String]` and `Headers.SensitiveHeaders`.
  - Zero sensitive data leaks: API responses strip message payload/OTP and device tokens; phone numbers and credentials are masked in logs and responses (`+7999****12`).

---

## Features

- **Cascade Failover**:
  - Tries fast / low-cost channels first (Push via APNS/FCM).
  - Automatically falls back to Primary SMS on timeout or transient errors.
  - Switches to Backup SMS provider if Primary SMS fails with transient error.
  - Terminates immediately on permanent failures (e.g. invalid phone number) without redundant calls.
  - Marketing notifications do not automatically spill into paid SMS channels.
- **Traffic Prioritization**:
  - `CRITICAL`: 2FA codes, billing alerts (bypass frequency caps).
  - `MARKETING`: Promos and updates (capped per user sliding window).
- **Atomic Idempotency**:
  - Deduplication via `idempotencyKey`. Parallel requests with the same key trigger delivery **exactly once** and return the same record.
- **Lease Timeout for Resiliency**:
  - Automatically detects stale `InProgress` records on retry to prevent permanently stuck deliveries.
- **Observability**:
  - Kubernetes probes: `/health/live` and `/health/ready`.
  - Type-safe OpenAPI / Swagger UI generated via **Tapir**.

---

## Tech Stack

- **Language:** Scala 3.3 LTS
- **Concurrency & Streaming:** Cats Effect 3, fs2, fs2-kafka
- **HTTP Engine:** Tapir, Http4s Ember, Circe
- **Logging:** Log4cats, Logback
- **Testing:** MUnit, Cats Effect Testing Suite

---

## Getting Started

### 1. Environment Setup
Copy the template and set a secure key (minimum 16 characters):
```bash
cp .env.example .env
# Edit .env and set APP_API_KEY
```

### 2. Running Locally
```bash
sbt run
```
The server starts on `http://localhost:8081`.
Interactive Swagger UI: **http://localhost:8081/docs**

### 3. Running Tests
```bash
sbt test
```

### 4. Running with Docker & Kafka
To start the application alongside Kafka and Kafka UI:
```bash
docker compose up --build -d
```
* **Application API:** `http://localhost:8081`
* **Swagger Documentation:** `http://localhost:8081/docs`
* **Kafka UI:** `http://localhost:8080`

---

## API Examples

### 1. Send Critical Notification (2FA)
```bash
curl -X POST http://localhost:8081/api/v1/notifications \
  -H "X-API-Key: your-secret-api-key" \
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
  -H "X-API-Key: your-secret-api-key" \
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

### 3. Health Probes
```bash
curl -i http://localhost:8081/health/live
curl -i http://localhost:8081/health/ready
```
