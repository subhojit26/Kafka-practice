package com.learnKafka.library_events_producer_v2.service;

import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.domain.LibraryEventType;
import com.learnKafka.library_events_producer_v2.producer.LibraryEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Business service for library events. Encapsulates business rules and delegates
 * publishing to the {@link LibraryEventProducer}.
 */
@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventService(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    /**
     * Handles the creation (ADD) flow for a library event.
     * <p>
     * Enforces the rule that the incoming event must have {@code eventType == ADD}
     * and delegates publishing to Kafka.
     *
     * @param event the incoming library event
     * @return the same event after publishing has been initiated
     * @throws IllegalArgumentException if the event type is not ADD
     */
    public LibraryEvent createLibraryEvent(LibraryEvent event) {

        if (event.eventType() != LibraryEventType.ADD) {
            throw new IllegalArgumentException("Only ADD event type is supported for create");
        }

        log.info("Creating libraryEvent with id={}", event.libraryEventId());
        libraryEventProducer.publishLibraryEvent(event);
        return event;
    }

    /**
     * Handles the update (UPDATE) flow for a library event.
     * <p>
     * Enforces the rules that the incoming event must have
     * {@code eventType == UPDATE} and a non-null {@code libraryEventId},
     * then delegates publishing to Kafka.
     *
     * @param event the incoming library event
     * @return the same event after publishing has been initiated
     * @throws IllegalArgumentException if the event type is not UPDATE or the id is null
     */
    public LibraryEvent updateLibraryEvent(LibraryEvent event) {

        if (event.eventType() != LibraryEventType.UPDATE) {
            throw new IllegalArgumentException("Only UPDATE event type is supported for update");
        }

        if (event.libraryEventId() == null) {
            throw new IllegalArgumentException("libraryEventId must not be null for update");
        }

        log.info("Updating libraryEvent with id={}", event.libraryEventId());
        libraryEventProducer.publishLibraryEvent(event);
        return event;
    }
}

