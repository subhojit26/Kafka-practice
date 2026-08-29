package com.learnKafka.library_events_producer_v2.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Represents a book associated with a library event.
 *
 * @param bookId     unique identifier of the book (required)
 * @param bookName   name/title of the book (required, non-blank)
 * @param bookAuthor author of the book (required, non-blank)
 */
public record Book(

        @NotNull(message = "book.bookId must not be null")
        Integer bookId,

        @NotBlank(message = "book.bookName must not be blank")
        String bookName,

        @NotBlank(message = "book.bookAuthor must not be blank")
        String bookAuthor
) {
}

