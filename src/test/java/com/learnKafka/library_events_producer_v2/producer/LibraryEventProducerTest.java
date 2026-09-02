package com.learnKafka.library_events_producer_v2.producer;

import com.learnKafka.library_events_producer_v2.domain.Book;
import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.domain.LibraryEventType;
import com.learnKafka.library_events_producer_v2.exception.LibraryEventPublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LibraryEventProducer}.
 * <p>
 * The {@link KafkaTemplate} is mocked so these tests focus purely on the
 * producer's own logic: topic/key derivation, delegating to the template, and
 * wrapping send-initiation failures in a {@link LibraryEventPublishException}.
 * No real Kafka is involved.
 */
@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    private static final String TOPIC = "library-events";

    @Mock
    private KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<Long> keyCaptor;

    private LibraryEventProducer producer;

    @BeforeEach
    void setUp() {
        producer = new LibraryEventProducer(kafkaTemplate, TOPIC);
    }

    private LibraryEvent event(Integer id) {
        return new LibraryEvent(id, LibraryEventType.ADD,
                new Book(123, "Kafka Basics", "John Doe"));
    }

    private CompletableFuture<SendResult<Long, LibraryEvent>> completedSendResult(LibraryEvent event) {
        SendResult<Long, LibraryEvent> sendResult = new SendResult<>(
                null,
                new RecordMetadata(new TopicPartition(TOPIC, 0), 0L, 0, 0L, 0, 0));
        return CompletableFuture.completedFuture(sendResult);
    }

    @Test
    @DisplayName("Publishes to the configured topic using libraryEventId as the key")
    void publishLibraryEvent_usesTopicAndKey() {
        LibraryEvent event = event(42);
        when(kafkaTemplate.send(eq(TOPIC), eq(42L), eq(event)))
                .thenReturn(completedSendResult(event));

        CompletableFuture<SendResult<Long, LibraryEvent>> future =
                producer.publishLibraryEvent(event);

        assertThat(future).isNotNull();
        assertThat(future.isCompletedExceptionally()).isFalse();
        verify(kafkaTemplate, times(1)).send(eq(TOPIC), keyCaptor.capture(), eq(event));
        assertThat(keyCaptor.getValue()).isEqualTo(42L);
    }

    @Test
    @DisplayName("Uses a null key when libraryEventId is null")
    void publishLibraryEvent_nullId_usesNullKey() {
        LibraryEvent event = event(null);
        when(kafkaTemplate.send(eq(TOPIC), isNull(), eq(event)))
                .thenReturn(completedSendResult(event));

        producer.publishLibraryEvent(event);

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), isNull(), eq(event));
    }

    @Test
    @DisplayName("Returns the future from the KafkaTemplate")
    void publishLibraryEvent_returnsTemplateFuture() {
        LibraryEvent event = event(7);
        CompletableFuture<SendResult<Long, LibraryEvent>> expected = completedSendResult(event);
        when(kafkaTemplate.send(eq(TOPIC), eq(7L), eq(event))).thenReturn(expected);

        CompletableFuture<SendResult<Long, LibraryEvent>> actual =
                producer.publishLibraryEvent(event);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    @DisplayName("Wraps a synchronous send failure in LibraryEventPublishException")
    void publishLibraryEvent_sendThrows_wrapsException() {
        LibraryEvent event = event(99);
        RuntimeException boom = new RuntimeException("broker down");
        when(kafkaTemplate.send(eq(TOPIC), eq(99L), eq(event))).thenThrow(boom);

        assertThatThrownBy(() -> producer.publishLibraryEvent(event))
                .isInstanceOf(LibraryEventPublishException.class)
                .hasMessageContaining("99")
                .hasCause(boom);

        verify(kafkaTemplate, times(1)).send(eq(TOPIC), any(), eq(event));
    }
}

