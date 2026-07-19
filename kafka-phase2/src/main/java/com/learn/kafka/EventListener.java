package com.learn.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consuming with Spring: annotate a method with @KafkaListener and Spring runs
 * the entire poll loop, deserialization, and offset commits for you in the
 * background. The whole Phase 1 ConsumerApp collapses into this one method.
 *
 * The group id and deserializers come from application.yml.
 */
@Component
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info("Received key={} | value='{}' | partition {} | offset {}",
                record.key(), record.value(), record.partition(), record.offset());
    }
}
