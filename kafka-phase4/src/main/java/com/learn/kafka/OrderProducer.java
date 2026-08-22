package com.learn.kafka;

import com.learn.kafka.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * The producer is now IDEMPOTENT (see application.yml: enable.idempotence=true).
 * That means if the client has to retry a send after a network hiccup, the broker
 * recognizes the duplicate and stores the record only once — so retries can't
 * create duplicate messages. This is the safe default for at-least-once systems.
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
        String key = order.customer() != null ? order.customer().id() : "unknown";
        kafkaTemplate.send(topic, key, order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Send FAILED for order {}: {}", order.orderId(), ex.getMessage());
                    } else {
                        var md = result.getRecordMetadata();
                        log.info("Sent order {} -> partition {}, offset {}",
                                order.orderId(), md.partition(), md.offset());
                    }
                });
    }
}
