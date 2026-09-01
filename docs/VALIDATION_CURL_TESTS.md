# Payload Validation — cURL Test Commands

Base URL: `http://localhost:8080`

Endpoints under test:

| Method | Path                 | Success | Rules                                             |
|--------|----------------------|---------|---------------------------------------------------|
| `POST` | `/v1/libraryevent`   | `201`   | `eventType == ADD`                                |
| `PUT`  | `/v1/libraryevent`   | `200`   | `eventType == UPDATE` **and** non-null `libraryEventId` |

## Prerequisites

1. Kafka must be running (see `compose.yaml`). Start it via Docker Compose or let
   Spring Boot's docker-compose support launch it automatically.
2. Start the application:
   ```powershell
   .\gradlew.bat bootRun
   ```
3. (Optional) Create the topic ahead of time with `create-topic.ps1`.

## How to read these examples

All commands below target Windows **PowerShell** (`curl.exe`, JSON quotes escaped
as `\"`). Each example lists the **expected HTTP status** and the **actual error
message returned in the response body**. Equivalent Bash/Linux/macOS variants are
provided at the bottom of the file.

## Contents

- **POST /v1/libraryevent (create flow)** — cases 1–11
- **PUT /v1/libraryevent (update flow)** — cases 12–14
- **Bash / Git Bash / Linux / macOS variants**

On any failure the API returns a JSON `ApiError` body:

```json
{
  "timestamp": "2026-08-30T10:15:30.123Z",
  "status": 400,
  "error": "Bad Request",
  "message": "book.bookId - book.bookId must not be null",
  "errors": ["book.bookId - book.bookId must not be null"],
  "path": "/v1/libraryevent"
}
```

- `message` — combined, sorted summary of all errors.
- `errors` — per-field list (empty for non-validation failures). When multiple
  fields fail validation, **every** failure is listed here.

Validation rules enforced:

| Field                 | Rule                                                        |
|-----------------------|-------------------------------------------------------------|
| `eventType`           | must not be null; `ADD` for POST, `UPDATE` for PUT          |
| `libraryEventId`      | must not be null for PUT (update)                           |
| `book`                | must not be null                                            |
| `book.bookId`         | must not be null                                            |
| `book.bookName`       | must not be blank                                           |
| `book.bookAuthor`     | must not be blank                                           |

---

## POST /v1/libraryevent (create flow)

## 1. Valid payload — expect `201 Created`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 2. Missing `eventType` — expect `400`

Body `message`: `eventType - eventType must not be null`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 3. Missing `book` — expect `400`

Body `message`: `book - book must not be null`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\" }'
```

## 4. Missing `book.bookId` — expect `400`

Body `message`: `book.bookId - book.bookId must not be null`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 5. Blank `book.bookName` — expect `400`

Body `message`: `book.bookName - book.bookName must not be blank`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookId\": 123, \"bookName\": \"   \", \"bookAuthor\": \"John Doe\" } }'
```

## 6. Blank `book.bookAuthor` — expect `400`

Body `message`: `book.bookAuthor - book.bookAuthor must not be blank`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"\" } }'
```

## 7. Multiple validation errors at once — expect `400`

`errors` contains every failure; `message` is the sorted, comma-joined summary:
`book.bookAuthor - book.bookAuthor must not be blank, book.bookId - book.bookId must not be null, book.bookName - book.bookName must not be blank`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookName\": \"\", \"bookAuthor\": \"\" } }'
```

## 8. Invalid `eventType` enum value — expect `400`

Body `message` starts with `Malformed or unreadable request body: ...`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"DELETE\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 9. Wrong `eventType` for POST (business rule) — expect `400`

Body `message`: `Only ADD event type is supported for POST`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": 1, \"eventType\": \"UPDATE\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 10. Wrong data type for `bookId` — expect `400`

Body `message` starts with `Malformed or unreadable request body: ...`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"ADD\", \"book\": { \"bookId\": \"abc\", \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

## 11. Empty request body — expect `400`

```powershell
curl.exe -i -X POST http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d ''
```

---

## PUT /v1/libraryevent (update flow)

### 12. Valid UPDATE — expect `200 OK`

```powershell
curl.exe -i -X PUT http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": 1, \"eventType\": \"UPDATE\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

### 13. Missing `libraryEventId` on PUT — expect `400`

Body `message`: `libraryEventId must not be null for update`

```powershell
curl.exe -i -X PUT http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": null, \"eventType\": \"UPDATE\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

### 14. Wrong event type for PUT (ADD) — expect `400`

Body `message`: `Only UPDATE event type is supported for PUT`

```powershell
curl.exe -i -X PUT http://localhost:8080/v1/libraryevent `
  -H "Content-Type: application/json" `
  -d '{ \"libraryEventId\": 1, \"eventType\": \"ADD\", \"book\": { \"bookId\": 123, \"bookName\": \"Kafka Basics\", \"bookAuthor\": \"John Doe\" } }'
```

---

## Bash / Git Bash / Linux / macOS variants

Single-quoted JSON (no escaping needed):

```bash
# Valid payload
curl -i -X POST http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": null, "eventType": "ADD", "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'

# Missing eventType
curl -i -X POST http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": null, "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'

# Multiple errors
curl -i -X POST http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": null, "eventType": "ADD", "book": { "bookName": "", "bookAuthor": "" } }'

# Wrong eventType for POST
curl -i -X POST http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": 1, "eventType": "UPDATE", "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'

# Valid UPDATE (PUT) -> 200
curl -i -X PUT http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": 1, "eventType": "UPDATE", "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'

# Missing libraryEventId on PUT -> 400
curl -i -X PUT http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": null, "eventType": "UPDATE", "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'

# Wrong event type for PUT -> 400
curl -i -X PUT http://localhost:8080/v1/libraryevent \
  -H "Content-Type: application/json" \
  -d '{ "libraryEventId": 1, "eventType": "ADD", "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }'
```

