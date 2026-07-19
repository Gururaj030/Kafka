package com.learn.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Producing with Spring: inject a KafkaTemplate and call send().
 * Compare this to all the Properties/KafkaProducer setup in Phase 1 —
 * Spring built and configured the template from application.yml for us.
 */
@Service
public class EventProducer {

    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    // Spring injects the auto-configured KafkaTemplate and our topic name.
    public EventProducer(KafkaTemplate<String, String> kafkaTemplate,
                         @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(String key, String message) {
        // send() is asynchronous and returns a CompletableFuture. We attach a
        // callback to log WHERE the record landed — same partition/offset idea
        // as Phase 1, now one line instead of a manual Callback.
        kafkaTemplate.send(topic, key, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Send FAILED for key={}: {}", key, ex.getMessage());
                    } else {
                        var md = result.getRecordMetadata();
                        log.info("Sent key={} -> partition {}, offset {}",
                                key, md.partition(), md.offset());
                    }
                });
    }
}
