package com.learn.kafka.model;

import java.util.List;

/** Same typed event from Phase 3 — reused so we can focus on reliability. */
public record OrderEvent(
        String orderId,
        Customer customer,
        List<LineItem> items,
        double total,
        String createdAt
) {
}
