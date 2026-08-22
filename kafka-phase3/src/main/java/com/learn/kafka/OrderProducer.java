package com.learn.kafka;

import com.learn.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Note the generic type: KafkaTemplate<String, OrderEvent>. We now send a typed
 * object, not a String. The JsonSerializer (configured in application.yml)
 * converts it to JSON bytes on the way out.
 */
@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String topic;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate,
                         @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(OrderEvent order) {
        // Key by customer id so all of one customer's orders keep their order
        // on a single partition (the Phase 1 rule, still true).
        String key = order.customer() != null ? order.customer().id() : "unknown";

        kafkaTemplate.send(topic, key, order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Send FAILED for order {}: {}", order.orderId(), ex.getMessage());
                    } else {
                        var md = result.getRecordMetadata();
                        log.info("Sent order {} (key={}) -> partition {}, offset {}",
                                order.orderId(), key, md.partition(), md.offset());
                    }
                });
    }
}
