package com.learnKafka.library_events_producer_v2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnKafka.library_events_producer_v2.domain.Book;
import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Full-stack integration test for {@link LibraryEventsController}.
 * <p>
 * The whole Spring Boot application context is started and wired to an
 * <b>embedded Kafka broker</b> (no external Kafka needed). Requests are
 * dispatched through {@link MockMvc}, exercising the real controller, service
 * and producer, and the published records are consumed back from the embedded
 * broker to assert the end-to-end flow:
 * <pre>HTTP request -&gt; controller -&gt; service -&gt; producer -&gt; Kafka topic</pre>
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(topics = LibraryEventsControllerIntegrationTest.TOPIC, partitions = 3)
class LibraryEventsControllerIntegrationTest {

    static final String TOPIC = "library-events";
    private static final String LIBRARY_EVENT_URL = "/v1/libraryevent";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Consumer<Long, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        // Unique group per test so consumers never share committed offsets.
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-group-" + System.nanoTime());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps, new LongDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TOPIC);

        // The topic is shared across tests in this class. Seek to the current end
        // so each test only observes records it produces itself.
        consumer.poll(Duration.ZERO);
        consumer.seekToEnd(consumer.assignment());
        for (TopicPartition partition : consumer.assignment()) {
            consumer.position(partition); // force the lazy seek to resolve
        }
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    private String toJson(LibraryEvent event) throws Exception {
        return objectMapper.writeValueAsString(event);
    }

    // ---------------------------------------------------------------------
    // POST /v1/libraryevent
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("POST valid ADD -> 201 and record published to embedded Kafka")
    void postLibraryEvent_publishesToKafka() throws Exception {
        LibraryEvent request = new LibraryEvent(
                null,
                LibraryEventType.ADD,
                new Book(123, "Kafka Basics", "John Doe"));

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isCreated());

        ConsumerRecord<Long, String> record =
                KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));

        assertThat(record).isNotNull();
        LibraryEvent published = objectMapper.readValue(record.value(), LibraryEvent.class);
        assertThat(published.eventType()).isEqualTo(LibraryEventType.ADD);
        assertThat(published.book().bookId()).isEqualTo(123);
        assertThat(published.book().bookName()).isEqualTo("Kafka Basics");
        assertThat(published.book().bookAuthor()).isEqualTo("John Doe");
    }

    @Test
    @DisplayName("POST invalid payload (missing eventType) -> 400 and nothing published")
    void postLibraryEvent_invalidPayload_returns400() throws Exception {
        LibraryEvent request = new LibraryEvent(
                null,
                null, // missing eventType -> validation failure
                new Book(123, "Kafka Basics", "John Doe"));

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isBadRequest());

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isZero();
    }

    // ---------------------------------------------------------------------
    // PUT /v1/libraryevent
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("PUT valid UPDATE -> 200 and record published with matching key")
    void putLibraryEvent_publishesToKafka() throws Exception {
        LibraryEvent request = new LibraryEvent(
                1,
                LibraryEventType.UPDATE,
                new Book(456, "Advanced Kafka", "Jane Roe"));

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isOk());

        ConsumerRecord<Long, String> record =
                KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(1L);
        LibraryEvent published = objectMapper.readValue(record.value(), LibraryEvent.class);
        assertThat(published.libraryEventId()).isEqualTo(1);
        assertThat(published.eventType()).isEqualTo(LibraryEventType.UPDATE);
        assertThat(published.book().bookId()).isEqualTo(456);
    }

    @Test
    @DisplayName("PUT without libraryEventId -> 400 and nothing published")
    void putLibraryEvent_missingId_returns400() throws Exception {
        LibraryEvent request = new LibraryEvent(
                null,
                LibraryEventType.UPDATE,
                new Book(456, "Advanced Kafka", "Jane Roe"));

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isBadRequest());

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(records.count()).isZero();
    }
}



