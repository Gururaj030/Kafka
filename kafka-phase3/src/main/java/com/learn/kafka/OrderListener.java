package com.learn.kafka;

import com.learn.kafka.model.LineItem;
import com.learn.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The listener method parameter is OrderEvent — Spring's JsonDeserializer
 * rebuilds the object from JSON bytes (using the type header) before your code
 * runs. You work with a real object, nested fields and all.
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    @KafkaListener(topics = "${app.topic}")
    public void onOrder(OrderEvent order) {
        log.info("Received order {} from {} <{}> — total ${}",
                order.orderId(),
                order.customer().name(),
                order.customer().email(),
                order.total());

        // Proof the nested list survived serialization: iterate the line items.
        for (LineItem item : order.items()) {
            log.info("    line: {} x{} @ ${}", item.sku(), item.quantity(), item.price());
        }
    }
}
