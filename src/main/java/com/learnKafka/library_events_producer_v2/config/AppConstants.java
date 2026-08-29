package com.learnKafka.library_events_producer_v2.config;

/**
 * Centralized application constants for API paths and default values.
 */
public final class AppConstants {

    private AppConstants() {
        // Prevent instantiation
    }

    /** Base path for all Library Event API endpoints. */
    public static final String API_BASE_PATH = "/v1";

    /** Library event resource path. */
    public static final String LIBRARY_EVENT_PATH = API_BASE_PATH + "/libraryevent";

    /** Config key for the Kafka topic name. */
    public static final String KAFKA_TOPIC_PROPERTY = "app.kafka.topic";

    /** Default Kafka topic used when none is configured. */
    public static final String DEFAULT_KAFKA_TOPIC = "library-events";
}

