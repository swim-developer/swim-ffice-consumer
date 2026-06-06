# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quarkus service that consumes FF-ICE (Flight and Flow Information for a Collaborative Environment) messages from an ATFM provider via AMQP 1.0, processes FIXM 4.3 / FF-ICE 1.1 XML, persists events to MongoDB, and dispatches them to Kafka topics by message category.

This is an **ANSP-side consumer** — it connects to `swim-ffice-consumer-validator` during dev/test. **Never connects to `swim-ffice-provider`.**

## Build & Run

```bash
# First-time setup: clone and install all sibling dependencies
make sync

# Build (skip tests)
make build                    # or: ./mvnw clean package -DskipTests

# Unit tests only
./mvnw test

# Unit + integration tests (uses Testcontainers: MongoDB, Kafka/Redpanda, WireMock, Artemis)
make test                     # or: ./mvnw verify -DskipITs=false

# Run a single test class
./mvnw test -Dtest=EventExtractorTest

# Run a single integration test
./mvnw verify -DskipITs=false -Dit.test=FficeConsumerIT

# Dev mode (requires compose.yml services running)
docker compose up -d          # or: podman compose up -d
./mvnw quarkus:dev
```

Integration tests are **skipped by default** (`<skipITs>true</skipITs>` in pom.xml). Use `-DskipITs=false` to enable them.

## Sibling Dependencies

This project depends on libraries that must be installed in the local Maven repository before building:

- `swim-developer-root` (parent POM, install with `-N`)
- `swim-fixm-ffice-model` (JAXB-generated FIXM/FF-ICE types)
- `swim-developer-framework` (hexagonal consumer framework: orchestrator, inbox/outbox, idempotency, self-healing)
- `swim-developer-extensions` (Kafka outbox routing, inbox reader)

Run `make sync` to clone and install all of them, or `make deps` to see manual steps.

## Architecture

Hexagonal architecture. Package root: `com.github.swim_developer`.

- **domain/model** — `Event` (MongoDB entity, implements `SwimOutboxEvent`), `Subscription`, `FilterDimension`
- **application/usecase** — `EventProcessingUseCase` (delegates to framework's `EventProcessingOrchestrator`), `SubscriptionUseCase`
- **application/service** — `EventDataValidator`, `EventFilterService`, `EventPersistenceService`, `ProcessingMetrics`, `ProcessorCallbacks`
- **application/port** — `in/ManageSubscriptionPort`, `out/EventStore`, `out/SubscriptionStore`, `out/RemoteSubscriptionManagerPort`
- **infrastructure/in/amqp** — `InboxMessageHandler` (Kafka inbox batch consumer, extends `AbstractKafkaInboxReader`)
- **infrastructure/in/rest** — REST resources for subscriptions, events, operations, features
- **infrastructure/out/xml** — `EventExtractor` (FIXM XML to domain), `XmlEnvelopeParser`, `JaxbUnmarshallerPool`
- **infrastructure/out/messaging** — `OutboxMessageHandler` (Vert.x event bus consumer, dispatches to Kafka)
- **infrastructure/out/persistence** — MongoDB stores, index initializer, Panache repositories
- **infrastructure/out/client** — REST client adapter for the Subscription Manager API
- **infrastructure/out/subscription** — `FficeSubscriptionRenewalStrategy`

### Message Flow

1. AMQP messages arrive from the provider and are batched into Kafka (`ffice-inbox-topic`) by the framework
2. `InboxMessageHandler` consumes Kafka batches, splits XML envelopes, delegates each to `EventProcessingUseCase`
3. The framework orchestrator: validates XML against FIXM XSD, extracts domain fields via `EventExtractor`, checks idempotency (L1 Caffeine + L2 MongoDB), applies subscription filters, persists to MongoDB
4. `OutboxMessageHandler` dispatches persisted events to category-specific Kafka topics (`ffice-flight-plan-topic`, `ffice-operations-topic`, etc.) via `FficeMessageClassifier`
5. Invalid XML goes to DLQ (MongoDB + `ffice-dlq-topic`)

### Key Framework Features (inherited, not implemented here)

- Two-tier idempotency cache (Caffeine L1 + MongoDB L2)
- Self-healing: automatic re-subscription when provider returns 404
- Subscription renewal with configurable threshold
- Per-subscription heartbeat monitoring
- Outbox pattern with retry and recovery

## Testing

- **Unit tests** (`src/test/java/.../unit/`): pure logic tests for `EventExtractor`, `FficeMessageClassifier`
- **Integration tests** (`src/test/java/.../integration/FficeConsumerIT.java`): full-stack test using Quarkus Dev Services (Testcontainers). Uses `@ConnectWireMock` to simulate the Subscription Manager REST API. Tests cover: subscription lifecycle, event processing pipeline, idempotency, DLQ routing, event classification, self-healing
- Test profile (`application-test.properties`): disables schedulers (999d delay), OTEL, OIDC, management endpoint; enables file logging to `target/app.log`

## Local Development Stack (compose.yml)

- MongoDB 8.2 on port 27019 (+ Mongo Express on 9084)
- Kafka (Apache Kafka 4.1) on port 9093 (+ AKHQ UI on 9085)
- Artemis AMQP broker on port 5673 (console on 8166)
- swim-ffice-consumer-validator on port 8086 (event generator + Subscription Manager simulator)

## Quality Targets

```bash
make sonar-up     # Start local SonarQube
make sonar        # Run analysis (runs ITs against FficeConsumerIT)
make sonar-down   # Stop SonarQube
```

## Code Conventions

- Uses Lombok (`@Getter`, `@Setter`, `@Slf4j`, `@NoArgsConstructor`, `@AllArgsConstructor`)
- JAXB elements are wrapped in `JAXBElement<T>` — always use `unwrap()` pattern to handle nulls
- Domain model uses `ObjectId` for MongoDB IDs
- Kafka channels follow naming: `out-{category}` for outgoing, `in-{name}` for incoming
- REST API base paths: `/api/v1/subscriptions` (consumer API), `/swim/v1/operational/*` (framework operational endpoints)
