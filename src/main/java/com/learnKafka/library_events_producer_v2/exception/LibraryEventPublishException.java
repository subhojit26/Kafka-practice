package com.learnKafka.library_events_producer_v2.exception;

/**
 * Thrown when a library event fails to be published to Kafka.
 * Mapped to a 500 response by the global exception handler (Layer 4).
 */
public class LibraryEventPublishException extends RuntimeException {

    public LibraryEventPublishException(String message, Throwable cause) {
        super(message, cause);
    }

    public LibraryEventPublishException(String message) {
        super(message);
    }
}

