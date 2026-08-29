package com.learnKafka.library_events_producer_v2.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Represents a library event to be published to Kafka.
 *
 * @param libraryEventId identifier of the event; may be {@code null} for ADD, required for UPDATE
 * @param eventType      the type of the event (ADD or UPDATE)
 * @param book           the associated book (required, cascaded validation)
 */
public record LibraryEvent(

        Integer libraryEventId,

        @NotNull(message = "eventType must not be null")
        LibraryEventType eventType,

        @NotNull(message = "book must not be null")
        @Valid
        Book book
) {
}

