# notification-routing-engine

Notification routing and delivery service with automatic failover cascade, rate limiting, and idempotency guarantees. Built on Scala 3 and Cats Effect.

---

## Features

- **Cascade Failover**:
  - Tries fast / low-cost channels first (Push via APNS/FCM).
  - Automatically falls back to Primary SMS on timeout or transient errors.
  - Switches to Backup SMS provider if the primary SMS gateway degrades.
- **Traffic Prioritization**:
  - `CRITICAL`: 2FA codes, billing and security alerts (zero queue lag, bypasses marketing limits).
  - `MARKETING`: Promos, digest campaigns (frequency-capped per user).
- **Frequency Capping**:
  - In-memory sliding window rate limiter to prevent spamming users with marketing notifications.
- **Idempotency**:
  - Deduplication via `idempotencyKey` prevents duplicate notifications on client retries.
- **Type-safe API & Documentation**:
  - Declarative HTTP endpoints via **Tapir**.
  - Auto-generated Swagger UI at `/docs`.

---

## Tech Stack

- **Language:** Scala 3.3
- **Concurrency & Effects:** Cats Effect 3, fs2
- **HTTP / REST:** Tapir, Http4s Ember, Circe
- **Database:** PostgreSQL (Skunk for pure FP async driver)
- **Configuration & Logging:** Ciris, Log4cats, Logback
- **Testing:** MUnit, Cats Effect Suite

---

## Project Structure

```text
src/
├── main/
│   ├── resources/
│   │   ├── application.conf
│   │   └── db/migration/V1__init.sql
│   └── scala/com/tbank/notification/
│       ├── Main.scala
│       ├── api/             # Tapir endpoints & Http4s routes
│       ├── config/          # Ciris config loaders
│       ├── domain/          # ADTs, models, errors
│       ├── engine/          # Routing engine & frequency capper
│       ├── provider/        # Channel providers (Push, SMS Alpha/Beta)
│       └── repository/      # Notification storage interface & in-memory/db impl
└── test/
    └── scala/com/tbank/notification/
        ├── FrequencyCapperSpec.scala
        └── RoutingEngineSpec.scala
```

---

## Getting Started

### Prerequisites
- JDK 21+
- sbt 1.9+
- Docker (optional, for PostgreSQL/Redis)

### Running tests
```bash
sbt test
```

### Running the server
```bash
# Start background services (optional)
docker compose up -d

# Run the application
sbt run
```

The server starts on `http://localhost:8080`.
Interactive Swagger UI is available at `http://localhost:8080/docs`.

---

## API Examples

### Send Critical Notification (2FA)
```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "auth-step-40291",
    "userId": "usr_91",
    "priority": "CRITICAL",
    "content": "Your login code: 9140",
    "phone": "+79991234567",
    "deviceToken": "device-token-abc"
  }'
```

### Cascade Failover (Simulated Push Failure)
```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "auth-step-40292",
    "userId": "usr_91",
    "priority": "CRITICAL",
    "content": "Password reset verification: 3341",
    "phone": "+79991234567",
    "deviceToken": "fail-transient-push-token"
  }'
```

### Get Notification by ID
```bash
curl -X GET http://localhost:8080/api/v1/notifications/<UUID>
```

### Get User Notification History
```bash
curl -X GET http://localhost:8080/api/v1/notifications/user/usr_91
```
