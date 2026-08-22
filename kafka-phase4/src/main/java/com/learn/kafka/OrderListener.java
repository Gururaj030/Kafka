package com.learn.kafka;

import com.learn.kafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * The heart of Phase 4: automatic retries with a dead-letter topic.
 *
 * @RetryableTopic tells Spring: if this method throws, don't block the partition —
 * instead forward the message to a RETRY topic and try again later, with backoff.
 * After the attempts are exhausted, the message is forwarded to the DEAD-LETTER
 * topic (orders-v4-dlt), where @DltHandler picks it up.
 *
 * This is "non-blocking" retry: a single poison message can't stall everything
 * behind it, because retries happen on separate topics.
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    @RetryableTopic(
            attempts = "4",                                   // 1 original + 3 retries
            backoff = @Backoff(delay = 1000, multiplier = 2.0), // 1s, 2s, 4s
            dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "${app.topic}")
    public void onOrder(OrderEvent order,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String fromTopic) {
        log.info("Processing order {} (from topic '{}')", order.orderId(), fromTopic);

        // Simulate a "poison" message: any order with a negative total fails.
        if (order.total() < 0) {
            throw new IllegalArgumentException(
                    "Invalid order " + order.orderId() + ": total is negative (" + order.total() + ")");
        }

        log.info("  OK — order {} processed successfully. total=${}", order.orderId(), order.total());
    }

    /**
     * Runs only for messages that failed every retry. In real life you'd alert,
     * store the message for inspection, or push it to a human review queue.
     */
    @DltHandler
    public void handleDlt(OrderEvent order,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String fromTopic) {
        log.error("DEAD-LETTER: order {} gave up after all retries and landed on '{}'. " +
                        "Needs manual attention.", order.orderId(), fromTopic);
    }
}
