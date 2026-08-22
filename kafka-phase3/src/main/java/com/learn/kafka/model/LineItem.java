package com.learn.kafka.model;

/** One line in the order — the list element type inside OrderEvent. */
public record LineItem(
        String sku,
        int quantity,
        double price
) {
}
