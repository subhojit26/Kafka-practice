---
name: controller-skill
description: Guidance for writing Spring Boot REST controllers in this Kafka producer app, with method-based event-type rules, @Valid payloads, ResponseEntity results, and integration-based return types (CompletableFuture for Kafka, plain types for DB).
---

# Controller design for the library events producer

Use this skill when creating or updating a `@RestController` in this repository.
Base every new controller on `controller/LibraryEventsController`, the canonical example.

## Goal

Write thin controllers that only:

1. accept and validate the request payload
2. enforce HTTP-method / business preconditions
3. delegate to a service
4. wrap the result in `ResponseEntity`

Error handling lives in the `@RestControllerAdvice` (`exception/GlobalExceptionHandler`), **never** in the controller.

## Required controller setup

Prefer this class stack, reusing `AppConstants` instead of hardcoding paths:

```java
@RestController
@RequestMapping(AppConstants.API_BASE_PATH) // "/v1"
public class LibraryEventsController {

	private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

	private final LibraryEventService libraryEventService;

	public LibraryEventsController(LibraryEventService libraryEventService) {
		this.libraryEventService = libraryEventService;
	}
}
```

### Why these choices matter

- `@RestController` + class-level `@RequestMapping(AppConstants.API_BASE_PATH)`
  - versioned base path (`/v1`); use `AppConstants.LIBRARY_EVENT_PATH` for the resource segment
- **constructor injection** (no `@Autowired` fields)
  - keeps the controller testable and dependencies final
- one SLF4J logger per class, and `log.info(...)` the incoming request

## Return-type rules (core of this skill)

The controller's return type is decided by the **downstream integration**:

- **Kafka / `KafkaTemplate` integration → return a `CompletableFuture`.**
  The producer's send is asynchronous, so the service returns
  `CompletableFuture<SendResult<Long, LibraryEvent>>` and the endpoint exposes it as
  `CompletableFuture<ResponseEntity<?>>`. Send-initiation failures are wrapped in
  `LibraryEventPublishException` → mapped to 500 by the advice.
- **Database integration → return the regular/plain type.**
  The service returns the domain object (e.g. `Book`) or a collection synchronously; wrap
  it directly in `ResponseEntity<Book>`. **Do not** wrap DB results in `CompletableFuture`.

In both cases the response body is wrapped in `ResponseEntity` with an explicit status
(`HttpStatus.CREATED` for create, `HttpStatus.OK` for read/update).

### Kafka endpoint pattern

```java
@PostMapping(AppConstants.LIBRARY_EVENT_PATH)
public CompletableFuture<ResponseEntity<LibraryEvent>> postLibraryEvent(
		@RequestBody @Valid LibraryEvent libraryEvent) {

	log.info("Received POST libraryEvent: {}", libraryEvent);

	// Business rule: POST is only allowed for ADD (re-checked in the service).
	if (libraryEvent.eventType() != LibraryEventType.ADD) {
		throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				"Only ADD event type is supported for POST");
	}

	return libraryEventService.createLibraryEvent(libraryEvent)
			.thenApply(result -> ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent));
}
```

For `PUT`, keep the async pattern, require a non-null `libraryEventId`, enforce
`eventType == UPDATE`, and return `200 OK`.

### DB endpoint pattern

```java
@GetMapping(AppConstants.LIBRARY_EVENT_PATH + "/{id}")
public ResponseEntity<Book> getBook(@PathVariable Integer id) {
	log.info("Received GET book id={}", id);
	Book book = bookService.findById(id); // throws -> mapped by the advice
	return ResponseEntity.status(HttpStatus.OK).body(book);
}
```

## Payload validation

- Annotate the body param with `@RequestBody @Valid`.
- Domain types are **Java records** with Jakarta constraints (`@NotNull`, `@NotBlank`, and
  `@Valid` on nested objects) — see `domain/LibraryEvent` and `domain/Book`.
- Preconditions the annotations can't express (id required for update, event-type matches
  the HTTP method) are thrown as `ResponseStatusException(HttpStatus.BAD_REQUEST, "...")`
  and **re-checked in the service** (defense in depth).

## Error handling (do NOT put in the controller)

All handling is centralized in `@RestControllerAdvice` producing the `ApiError` record
(`timestamp, status, error, message, errors[], path`). Reuse these existing mappings:

- `MethodArgumentNotValidException` → 400 (sorted, comma-joined field messages)
- `HttpMessageNotReadableException` → 400 (malformed body / bad enum)
- `ResponseStatusException` → its own status
- `LibraryEventPublishException` → 500
- `Exception` catch-all → 500 with a non-leaking message

If a new integration adds a failure mode, add a handler **in the advice**, not the controller.

## Project conventions

- Keep controllers thin; push all business rules and integration into the service layer.
- `POST` publishes/creates `ADD`; `PUT` publishes/updates `UPDATE` and requires `libraryEventId`.
- Kafka endpoints return `CompletableFuture`; DB endpoints return the plain type.
- Always wrap the result in `ResponseEntity` with an explicit status.
- Reuse `AppConstants.API_BASE_PATH` and `AppConstants.LIBRARY_EVENT_PATH`; never hardcode paths.
- No try/catch or error-response building in the controller — advice + `ApiError` only.

## Reference implementation in this repo

Base your changes on:

- `src/main/java/com/learnKafka/library_events_producer_v2/controller/LibraryEventsController.java`
- `src/main/java/com/learnKafka/library_events_producer_v2/exception/GlobalExceptionHandler.java`

## Quick template

```java
@RestController
@RequestMapping(AppConstants.API_BASE_PATH)
public class SomeController {

	private static final Logger log = LoggerFactory.getLogger(SomeController.class);

	private final SomeService someService;

	public SomeController(SomeService someService) {
		this.someService = someService;
	}

	// Kafka integration -> CompletableFuture<ResponseEntity<?>>
	// DB integration    -> ResponseEntity<PlainType>
	// @RequestBody @Valid payload; business rules -> ResponseStatusException
	// errors handled by @RestControllerAdvice (ApiError)
}
```

