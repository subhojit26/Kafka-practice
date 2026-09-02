package com.learnKafka.library_events_producer_v2.producer;

import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.exception.LibraryEventPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link LibraryEvent} messages to Kafka.
 * <p>
 * The topic name is read from the YAML property {@code spring.kafka.template.default-topic}
 * and the message key is the {@code libraryEventId}.
 */
@Component
public class LibraryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventProducer.class);

    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    private final String topic;

    public LibraryEventProducer(KafkaTemplate<Long, LibraryEvent> kafkaTemplate,
                                @Value("${spring.kafka.template.default-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Asynchronously publishes the given library event to Kafka using
     * {@code libraryEventId} as the message key.
     *
     * @param libraryEvent the event to publish
     * @return a future completing with the send result
     * @throws LibraryEventPublishException if the send cannot be initiated
     */
    public CompletableFuture<SendResult<Long, LibraryEvent>> publishLibraryEvent(LibraryEvent libraryEvent) {

        Long key = libraryEvent.libraryEventId() != null
                ? libraryEvent.libraryEventId().longValue()
                : null;

        log.info("Publishing libraryEvent to topic '{}' with key={}, eventType={}",
                topic, key, libraryEvent.eventType());

        CompletableFuture<SendResult<Long, LibraryEvent>> future;
        try {
            future = kafkaTemplate.send(topic, key, libraryEvent);
        } catch (Exception ex) {
            log.error("Failed to initiate publish for key={}: {}", key, ex.getMessage(), ex);
            throw new LibraryEventPublishException(
                    "Failed to initiate publish for libraryEventId=" + key, ex);
        }

        future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                handleFailure(key, throwable);
            } else {
                handleSuccess(key, result);
            }
        });

        return future;
    }

    private void handleSuccess(Long key, SendResult<Long, LibraryEvent> result) {
        log.info("Publish succeeded for key={}, partition={}, offset={}",
                key,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }

    private void handleFailure(Long key, Throwable throwable) {
        log.error("Publish failed for key={}: {}", key, throwable.getMessage(), throwable);
    }
}

