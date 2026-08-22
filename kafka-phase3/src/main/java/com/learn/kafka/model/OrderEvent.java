package com.learn.kafka.model;

import java.util.List;

/**
 * A real, typed event — not a String. This is what flows through Kafka in Phase 3.
 *
 * Java records are perfect here: immutable, concise, and Jackson (Spring's JSON
 * library) serializes/deserializes them automatically. Note the NESTED shape —
 * a Customer object and a List of line items — to prove nested JSON round-trips.
 */
public record OrderEvent(
        String orderId,
        Customer customer,
        List<LineItem> items,
        double total,
        String createdAt   // ISO timestamp as a String (kept simple on purpose)
) {
}
