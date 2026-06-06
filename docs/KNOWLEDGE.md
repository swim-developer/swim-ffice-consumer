# swim-ffice-consumer — Knowledge Base


## What This Is

**ANSP role.** Consumes FF-ICE (Flight and Flow Information for a Collaborative Environment) messages from an ATFM provider via AMQP 1.0. Data model: FIXM FF-ICE (from `swim-fixm-ffice-model`). Same framework patterns as DNOTAM and ED-254 consumers.

## CRITICAL: Who This Connects To

**NEVER connects to `swim-ffice-provider`.** During dev/test → `swim-ffice-consumer-validator`.

## Architecture

Same hexagonal structure as all consumers. Package root: `com.github.swim_developer.ffice.consumer`.

All resilience, heartbeat, self-healing, and multi-provider features are inherited from `swim-developer-framework`.

## Build & Run

```bash
cd ../swim-developer-framework && mvn clean install -DskipTests
./mvnw clean package -DskipTests
quarkus dev
./mvnw verify -DskipITs=false
```
