package com.learnKafka.library_events_producer_v2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LibraryEventsProducerV2Application {

	public static void main(String[] args) {
		SpringApplication.run(LibraryEventsProducerV2Application.class, args);
		System.out.println("LibraryEventsProducerV2Application started");
	}

}
