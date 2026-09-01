package com.learnKafka.library_events_producer_v2.exception;

import java.time.Instant;
import java.util.List;

/**
 * Standard error response body returned for all handled exceptions.
 *
 * @param timestamp when the error occurred (ISO-8601)
 * @param status    the HTTP status code
 * @param error     the HTTP status reason phrase (e.g. "Bad Request")
 * @param message   the actual, human-readable error message
 * @param errors    per-field validation errors (empty for non-validation failures)
 * @param path      the request path that produced the error
 */
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        List<String> errors,
        String path
) {
}

