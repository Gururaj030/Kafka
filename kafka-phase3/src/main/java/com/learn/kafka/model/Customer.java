package com.learn.kafka.model;

/** Nested object inside OrderEvent. */
public record Customer(
        String id,
        String name,
        String email
) {
}
