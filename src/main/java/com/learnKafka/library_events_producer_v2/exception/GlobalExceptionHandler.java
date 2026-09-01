package com.learnKafka.library_events_producer_v2.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Global exception handler that converts validation and business-rule failures
 * into consistent {@link ApiError} JSON responses containing the actual error message.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles bean-validation (@Valid) failures on the request body.
     * Every field error is included in {@code errors}, and a combined,
     * deterministic (sorted) summary is placed in {@code message}.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidationErrors(MethodArgumentNotValidException ex,
                                                           HttpServletRequest request) {
        // Collect ALL failures: per-field errors AND class-level (global) errors,
        // so every validation problem is reported in a single response.
        Stream<String> fieldMessages = ex.getBindingResult().getFieldErrors()
                .stream()
                .map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage());

        Stream<String> globalMessages = ex.getBindingResult().getGlobalErrors()
                .stream()
                .map(globalError -> globalError.getObjectName() + " - " + globalError.getDefaultMessage());

        List<String> allErrors = Stream.concat(fieldMessages, globalMessages)
                .sorted()
                .collect(Collectors.toList());

        String message = String.join(", ", allErrors);
        log.warn("Validation error(s) [{} total]: {}", allErrors.size(), message);
        return build(HttpStatus.BAD_REQUEST, message, allErrors, request);
    }

    /**
     * Handles malformed / unparseable JSON payloads (e.g., invalid enum value,
     * wrong data type, empty body).
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadableMessage(HttpMessageNotReadableException ex,
                                                            HttpServletRequest request) {
        String message = "Malformed or unreadable request body: " + ex.getMostSpecificCause().getMessage();
        log.warn(message);
        return build(HttpStatus.BAD_REQUEST, message, List.of(), request);
    }

    /**
     * Handles business-rule violations raised via {@link ResponseStatusException}.
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> handleResponseStatus(ResponseStatusException ex,
                                                        HttpServletRequest request) {
        log.warn("Business rule violation: {}", ex.getReason());
        HttpStatus status = HttpStatus.valueOf(ex.getStatusCode().value());
        return build(status, ex.getReason(), List.of(), request);
    }

    /**
     * Handles failures while publishing to Kafka.
     */
    @ExceptionHandler(LibraryEventPublishException.class)
    public ResponseEntity<ApiError> handlePublishFailure(LibraryEventPublishException ex,
                                                        HttpServletRequest request) {
        log.error("Publish failure: {}", ex.getMessage(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), List.of(), request);
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message,
                                           List<String> errors, HttpServletRequest request) {
        ApiError body = new ApiError(
                Instant.now(),
                status.value(),
                status.getReasonPhrase(),
                message,
                errors,
                request.getRequestURI()
        );
        return ResponseEntity.status(status).body(body);
    }
}




