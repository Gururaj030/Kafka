package com.learn.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Only the MAIN topic is declared here. The retry topics (orders-v4-retry-0, …)
 * and the dead-letter topic (orders-v4-dlt) are created automatically by the
 * retry infrastructure enabled in RetryConfig.
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic ordersTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
