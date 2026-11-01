# notification-routing-engine

Notification routing and delivery service with automatic failover cascade, rate limiting, and idempotency guarantees. Built with Scala 3, Cats Effect 3, fs2-kafka, and Tapir.

---

## Features

- **Cascade Failover**:
  - Tries fast / low-cost channels first (Push via APNS/FCM).
  - Automatically falls back to Primary SMS on timeout or transient errors.
  - Switches to Backup SMS provider if the primary SMS gateway degrades.
- **Traffic Prioritization**:
  - `CRITICAL`: 2FA codes, billing and security alerts (zero queue lag, bypasses marketing limits).
  - `MARKETING`: Promos, digest campaigns (frequency-capped per user via sliding window).
- **Event-Driven Architecture (Kafka)**:
  - Reactive ingestion from Kafka topic `notifications.requests` using `fs2-kafka`.
  - Batching & At-least-once delivery guarantees with manual offset commits.
  - Poison-pill & DLQ routing (`notifications.dlq`).
- **Observability & Health Probes**:
  - Kubernetes probes: `/health/live` and `/health/ready`.
  - Structured logging with MDC tracing.
- **Type-safe API & Documentation**:
  - Declarative HTTP endpoints via **Tapir**.
  - Interactive Swagger UI at `/docs`.

---

## Tech Stack

- **Language:** Scala 3.3 LTS
- **Concurrency & Streaming:** Cats Effect 3, fs2, fs2-kafka
- **HTTP / REST:** Tapir, Http4s Ember, Circe
- **Message Broker:** Apache Kafka (KRaft mode)
- **Database / Cache:** PostgreSQL (Skunk), Redis
- **Containerization:** Multi-stage Docker, Docker Compose

---

## Architecture

```text
[ REST API / Tapir ]          [ Kafka Topic: notifications.requests ]
        │                                         │
        │                                         ▼
        │                               [ fs2-kafka Consumer ]
        │                                         │
        └───────────────► ┌─────────────────────┐ ◄───┘
                          │    RoutingEngine    │
                          └──────────┬──────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
             (1) Try Push     (2) Primary SMS    (3) Backup SMS
              [ APNS/FCM ]      [ SMS-Alpha ]      [ SMS-Beta ]
```

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
│       ├── config/          # Ciris configuration loaders
│       ├── domain/          # ADTs, models, errors
│       ├── engine/          # Routing engine & frequency capper
│       ├── kafka/           # fs2-kafka consumer worker & stream pipeline
│       ├── provider/        # Channel providers (Push, SMS Alpha/Beta)
│       └── repository/      # Notification storage interface & in-memory/db impl
└── test/
    └── scala/com/tbank/notification/
        ├── FrequencyCapperSpec.scala
        └── RoutingEngineSpec.scala
```

---

## Getting Started

### Local Development via Docker Compose
To launch the complete infrastructure (Kafka, Kafka UI, PostgreSQL, Redis, Application):
```bash
docker compose up --build -d
```

Services:
* **Application API:** `http://localhost:8081`
* **Interactive Swagger UI:** `http://localhost:8081/docs`
* **Kafka UI:** `http://localhost:8080` (Inspect topics, consumer groups, and messages)
* **Kafka Broker:** `localhost:9092`
* **PostgreSQL:** `localhost:5432`

### Running Locally without Docker
```bash
sbt run
```

### Running Tests
```bash
sbt test
```

---

## API Examples

### Send Critical Notification (2FA)
```bash
curl -X POST http://localhost:8081/api/v1/notifications \
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
curl -X POST http://localhost:8081/api/v1/notifications \
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

### Health Probes (K8s)
```bash
curl -i http://localhost:8081/health/live
curl -i http://localhost:8081/health/ready
```
