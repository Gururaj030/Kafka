package com.learn.kafka;

import com.learn.kafka.model.OrderEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST a JSON order and it gets published to Kafka as a typed OrderEvent.
 * Spring binds the incoming JSON body straight into the OrderEvent record
 * (also via Jackson), so the same object shape is used at the HTTP edge and
 * on the Kafka wire.
 *
 * See PHASE3_GUIDE.md for a ready-to-paste curl with a full order body.
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
        return ResponseEntity.accepted()
                .body("Accepted order " + order.orderId() + "\n");
    }
}
