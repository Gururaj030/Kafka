package com.learn.kafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The request-to-event bridge: a REST endpoint that publishes to Kafka.
 * This is THE pattern you'll use constantly as a backend developer —
 * an HTTP call comes in, an event goes out.
 *
 * Try it:
 *   curl -X POST "http://localhost:8081/events?key=customer-1" \
 *        -H "Content-Type: text/plain" -d "hello from REST"
 */
@RestController
public class EventController {

    private final EventProducer producer;

    public EventController(EventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/events")
    public ResponseEntity<String> publish(@RequestParam(defaultValue = "default-key") String key,
                                          @RequestBody String message) {
        producer.send(key, message);
        return ResponseEntity.accepted()
                .body("Published (key=" + key + "): " + message + "\n");
    }
}
