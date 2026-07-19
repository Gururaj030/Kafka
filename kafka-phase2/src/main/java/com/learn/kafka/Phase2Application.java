package com.learn.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * PHASE 2 — Spring Boot entry point.
 *
 * @SpringBootApplication turns this into a full auto-configured app. Because
 * spring-kafka is on the classpath and application.yml has kafka settings,
 * Spring builds a KafkaTemplate and the listener machinery for you at startup.
 */
@SpringBootApplication
public class Phase2Application {
    public static void main(String[] args) {
        SpringApplication.run(Phase2Application.class, args);
    }
}
