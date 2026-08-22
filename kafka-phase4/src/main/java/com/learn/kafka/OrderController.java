package com.learn.kafka;

import com.learn.kafka.model.OrderEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST a good order (positive total) to see it processed once.
 * POST a "poison" order (negative total) to watch it retry and land in the DLT.
 * See PHASE4_GUIDE.md for ready-to-paste curls.
 */
@RestController
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> create(@RequestBody OrderEvent order) {
        producer.send(order);
        return ResponseEntity.accepted().body("Accepted order " + order.orderId() + "\n");
    }
}
