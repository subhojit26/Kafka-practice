package com.learnKafka.library_events_producer_v2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnKafka.library_events_producer_v2.domain.Book;
import com.learnKafka.library_events_producer_v2.domain.LibraryEvent;
import com.learnKafka.library_events_producer_v2.domain.LibraryEventType;
import com.learnKafka.library_events_producer_v2.service.LibraryEventService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer slice tests for {@link LibraryEventsController}.
 * <p>
 * The {@link LibraryEventService} is mocked so these tests focus purely on the
 * controller: request binding, bean validation ({@code @Valid}), the business
 * rules, and the {@code ApiError} responses produced by the global exception
 * handler. Kafka is never involved.
 * <p>
 * This class is intended to hold tests for <b>both</b> the POST and PUT
 * endpoints. Currently it covers the POST /v1/libraryevent endpoint; PUT tests
 * will be added here as well.
 */
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerUnitTest {

    private static final String LIBRARY_EVENT_URL = "/v1/libraryevent";

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @MockitoBean
    private LibraryEventService libraryEventService;

    private String toJson(Object value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }

    // =====================================================================
    // POST /v1/libraryevent
    // =====================================================================

    // ---------------------------------------------------------------------
    // Success path
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Valid ADD payload -> 201 Created and service invoked once")
    void postLibraryEvent_validAddPayload_returns201() throws Exception {
        LibraryEvent request = new LibraryEvent(
                null,
                LibraryEventType.ADD,
                new Book(123, "Kafka Basics", "John Doe"));

        when(libraryEventService.createLibraryEvent(any(LibraryEvent.class))).thenReturn(request);

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(123))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Basics"))
                .andExpect(jsonPath("$.book.bookAuthor").value("John Doe"));

        verify(libraryEventService, times(1)).createLibraryEvent(any(LibraryEvent.class));
    }

    // ---------------------------------------------------------------------
    // Validation failures (@Valid) -> 400, service never called
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Missing eventType -> 400 with field error")
    void postLibraryEvent_missingEventType_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null,
                  "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("eventType - eventType must not be null"));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("Missing book -> 400 with field error")
    void postLibraryEvent_missingBook_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null, "eventType": "ADD" }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("book - book must not be null"));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("Missing book.bookId -> 400 with nested field error")
    void postLibraryEvent_missingBookId_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null, "eventType": "ADD",
                  "book": { "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("book.bookId - book.bookId must not be null"));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("Blank book.bookName -> 400 with nested field error")
    void postLibraryEvent_blankBookName_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null, "eventType": "ADD",
                  "book": { "bookId": 123, "bookName": "   ", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("book.bookName - book.bookName must not be blank"));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("Multiple validation errors -> 400 with sorted summary and full errors list")
    void postLibraryEvent_multipleErrors_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null, "eventType": "ADD",
                  "book": { "bookName": "", "bookAuthor": "" } }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(
                        "book.bookAuthor - book.bookAuthor must not be blank, "
                                + "book.bookId - book.bookId must not be null, "
                                + "book.bookName - book.bookName must not be blank"))
                .andExpect(jsonPath("$.errors.length()").value(3));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    // ---------------------------------------------------------------------
    // Malformed / unreadable body -> 400
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Invalid eventType enum value -> 400 malformed body")
    void postLibraryEvent_invalidEnum_returns400() throws Exception {
        String payload = """
                { "libraryEventId": null, "eventType": "DELETE",
                  "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(
                        org.hamcrest.Matchers.startsWith("Malformed or unreadable request body:")));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("Empty request body -> 400 malformed body")
    void postLibraryEvent_emptyBody_returns400() throws Exception {
        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(""))
                .andExpect(status().isBadRequest());

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    // ---------------------------------------------------------------------
    // Business rule: POST only supports ADD -> 400
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("UPDATE eventType on POST -> 400 business rule violation")
    void postLibraryEvent_updateEventType_returns400() throws Exception {
        LibraryEvent request = new LibraryEvent(
                1,
                LibraryEventType.UPDATE,
                new Book(123, "Kafka Basics", "John Doe"));

        mockMvc.perform(post(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Only ADD event type is supported for POST"));

        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    // =====================================================================
    // PUT /v1/libraryevent
    // =====================================================================

    // ---------------------------------------------------------------------
    // Success path
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Valid UPDATE payload -> 200 OK and service invoked once")
    void putLibraryEvent_validUpdatePayload_returns200() throws Exception {
        LibraryEvent request = new LibraryEvent(
                1,
                LibraryEventType.UPDATE,
                new Book(123, "Kafka Basics", "John Doe"));

        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class))).thenReturn(request);

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.eventType").value("UPDATE"))
                .andExpect(jsonPath("$.book.bookId").value(123))
                .andExpect(jsonPath("$.book.bookName").value("Kafka Basics"))
                .andExpect(jsonPath("$.book.bookAuthor").value("John Doe"));

        verify(libraryEventService, times(1)).updateLibraryEvent(any(LibraryEvent.class));
    }

    // ---------------------------------------------------------------------
    // Validation failures (@Valid) -> 400, service never called
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Missing eventType on PUT -> 400 with field error")
    void putLibraryEvent_missingEventType_returns400() throws Exception {
        String payload = """
                { "libraryEventId": 1,
                  "book": { "bookId": 123, "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("eventType - eventType must not be null"));

        verify(libraryEventService, never()).updateLibraryEvent(any());
    }

    @Test
    @DisplayName("Missing book.bookId on PUT -> 400 with nested field error")
    void putLibraryEvent_missingBookId_returns400() throws Exception {
        String payload = """
                { "libraryEventId": 1, "eventType": "UPDATE",
                  "book": { "bookName": "Kafka Basics", "bookAuthor": "John Doe" } }
                """;

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("book.bookId - book.bookId must not be null"));

        verify(libraryEventService, never()).updateLibraryEvent(any());
    }

    // ---------------------------------------------------------------------
    // Business rule: PUT requires a non-null libraryEventId -> 400
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("Missing libraryEventId on PUT -> 400 business rule violation")
    void putLibraryEvent_missingLibraryEventId_returns400() throws Exception {
        LibraryEvent request = new LibraryEvent(
                null,
                LibraryEventType.UPDATE,
                new Book(123, "Kafka Basics", "John Doe"));

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("libraryEventId must not be null for update"));

        verify(libraryEventService, never()).updateLibraryEvent(any());
    }

    // ---------------------------------------------------------------------
    // Business rule: PUT only supports UPDATE -> 400
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("ADD eventType on PUT -> 400 business rule violation")
    void putLibraryEvent_addEventType_returns400() throws Exception {
        LibraryEvent request = new LibraryEvent(
                1,
                LibraryEventType.ADD,
                new Book(123, "Kafka Basics", "John Doe"));

        mockMvc.perform(put(LIBRARY_EVENT_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Only UPDATE event type is supported for PUT"));

        verify(libraryEventService, never()).updateLibraryEvent(any());
    }
}

