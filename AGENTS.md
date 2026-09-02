# AGENTS.md — Library Events Producer v2

Spring Boot 4 / Java 25 REST API that validates library events and publishes them to Kafka.
There is **no consumer, no database, no auth** in this service — it is a pure event producer.

## Architecture & Data Flow
Single linear flow, one class per layer:
`HTTP → LibraryEventsController → LibraryEventService → LibraryEventProducer → Kafka topic "library-events"`

- `controller/LibraryEventsController` — `@RestController` at `/v1`, endpoints `POST` + `PUT /v1/libraryevent` (note: singular `libraryevent`, **not** `library-events` as the PRD draft shows).
- `service/LibraryEventService` — enforces business rules **again** after the controller (defense in depth); throws `IllegalArgumentException`.
- `producer/LibraryEventProducer` — async `KafkaTemplate.send(topic, key, event)`; key = `libraryEventId`; success/failure logged in `whenComplete`. Wraps send-initiation errors in `LibraryEventPublishException`.
- `domain/` — `LibraryEvent`/`Book` are Java **records** with Jakarta `@NotNull`/`@Valid` validation; `LibraryEventType` enum (`ADD`, `UPDATE`).
- `exception/GlobalExceptionHandler` — `@RestControllerAdvice` mapping all errors to the `ApiError` record (timestamp, status, message, errors[], path).

## Project-Specific Conventions
- **Event-type is forced by HTTP method:** `POST` requires `eventType == ADD`, `PUT` requires `eventType == UPDATE` **and** non-null `libraryEventId` — else `400`. Rules are checked in *both* controller and service; keep them in sync.
- **Error mapping (see `GlobalExceptionHandler`):** `@Valid` failures → 400 with sorted, comma-joined field messages; `ResponseStatusException` (business rules) → its status; `LibraryEventPublishException` → 500; catch-all → 500 with a non-leaking message.
- `libraryEventId` is `Integer` in `LibraryEvent` but the Kafka **key is `Long`** (`.longValue()`); serializers are `LongSerializer` / `JsonSerializer` (`application.yml`).
- Config is profile-based: `application.yml` holds shared Kafka producer settings (`acks: all`, `retries: 10`); only `bootstrap-servers` varies per `application-{dev,stage,prod}.yml`. Default profile is `dev`.
- `jackson-databind` is **pinned to 2.20.2** in `build.gradle` for Kafka JsonSerializer compatibility — do not bump independently (see the comment there).

## Developer Workflows
- Build / test: `./gradlew build`, `./gradlew test` (JUnit 5). Run one test: `./gradlew test --tests "*LibraryEventProducerTest"`.
- Run app: `./gradlew bootRun` (uses `dev` profile → `localhost:9092`). `spring-boot-docker-compose` auto-starts `compose.yaml` (3-broker KRaft cluster) in dev.
- Create the Kafka topic on a host cluster: `./create-topic.ps1` (downloads Kafka CLI to `C:\kafka-cli`, RF=3, 3 partitions).
- Override profile: `--spring.profiles.active=stage` or `SPRING_PROFILES_ACTIVE=prod`.
- Health/metrics: `/actuator/health`, `/info`, `/metrics`.

## Testing Patterns
- Integration tests use `@SpringBootTest` + `@AutoConfigureMockMvc` + `@ActiveProfiles("test")` + `@EmbeddedKafka` — **no external Kafka needed**. `application-test.yml` points clients at `${spring.embedded.kafka.brokers}`.
- Tests consume records back from the embedded broker with a unique `GROUP_ID` per test and `seekToEnd` so each test only sees its own records (see `LibraryEventsControllerIntegrationTest`).
- Test tiers: `*UnitTest` (controller logic), `*IntegrationTest` (full HTTP→Kafka), `*ProducerTest`. Negative tests assert `records.count()` is zero when a request is rejected.

## Key Files
`build.gradle` · `compose.yaml` · `create-topic.ps1` · `src/main/resources/application*.yml` · `docs/PRD.md` · `docs/openapi.yaml`

