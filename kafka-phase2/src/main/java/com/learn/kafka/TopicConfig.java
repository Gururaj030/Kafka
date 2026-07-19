package com.learn.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaring a NewTopic bean makes Spring create the topic on startup
 * (via an admin client) if it doesn't already exist. Handy so you don't
 * have to create it by hand — and it gives us 3 partitions to play with.
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic appEventsTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
