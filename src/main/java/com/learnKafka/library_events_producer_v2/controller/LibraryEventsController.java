package com.learnKafka.library_events_producer_v2.controller;

import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.domain.LibraryEventType;
import com.learnKafka.library_events_producer_v2.service.LibraryEventService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * REST controller exposing the Library Events Producer API.
 */
@RestController
@RequestMapping("/v1")
public class LibraryEventsController {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    /**
     * POST /v1/libraryevent
     * <p>
     * Accepts a new library event. The request body is validated with {@code @Valid},
     * and the business rule {@code eventType == ADD} is enforced (otherwise 400).
     *
     * @param libraryEvent the incoming library event payload
     * @return 201 Created with the full library event payload
     */
    @PostMapping("/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {

        log.info("Received POST libraryEvent: {}", libraryEvent);

        // Business rule: POST is only allowed for ADD events.
        if (libraryEvent.eventType() != LibraryEventType.ADD) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Only ADD event type is supported for POST");
        }

        // Delegate business rule enforcement + Kafka publishing to the service layer.
        LibraryEvent created = libraryEventService.createLibraryEvent(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    /**
     * PUT /v1/libraryevent
     * <p>
     * Accepts an update to an existing library event. The request body is validated
     * with {@code @Valid}. The business rules {@code eventType == UPDATE} and a
     * non-null {@code libraryEventId} are enforced (otherwise 400).
     *
     * @param libraryEvent the incoming library event payload
     * @return 200 OK with the full library event payload
     */
    @PutMapping("/libraryevent")
    public ResponseEntity<LibraryEvent> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {

        log.info("Received PUT libraryEvent: {}", libraryEvent);

        // Business rule: PUT requires a non-null libraryEventId.
        if (libraryEvent.libraryEventId() == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "libraryEventId must not be null for update");
        }

        // Business rule: PUT is only allowed for UPDATE events.
        if (libraryEvent.eventType() != LibraryEventType.UPDATE) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Only UPDATE event type is supported for PUT");
        }

        // Delegate business rule enforcement + Kafka publishing to the service layer.
        LibraryEvent updated = libraryEventService.updateLibraryEvent(libraryEvent);

        return ResponseEntity.status(HttpStatus.OK).body(updated);
    }
}

