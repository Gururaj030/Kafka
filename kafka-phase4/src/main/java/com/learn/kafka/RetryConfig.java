package com.learn.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * This single annotation bootstraps Spring's non-blocking retry infrastructure.
 * Without it, @RetryableTopic on a listener does nothing. It lets Spring create
 * the retry topics and the dead-letter topic, and wire up the KafkaTemplate that
 * forwards failed messages between them.
 */
@Configuration
@EnableKafkaRetryTopic
public class RetryConfig {

    // Non-blocking retry scheduling (delaying redelivery attempts) needs a
    // TaskScheduler bean; Spring Boot doesn't auto-configure one by default.
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("retry-topic-scheduler-");
        scheduler.initialize();
        return scheduler;
    }
}
