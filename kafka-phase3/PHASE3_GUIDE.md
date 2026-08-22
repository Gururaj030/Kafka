# Phase 3 — Real-World Serialization with JSON

Goal: stop sending Strings and start sending **typed objects**. You'll produce
and consume a real `OrderEvent` POJO — with a nested `Customer` and a list of
`LineItem`s — end to end, and understand the type-header and trusted-packages
machinery that makes it work. At the end is a conceptual look at the Schema
Registry, which is where teams go next.

Keep Phase 0 Kafka running. This app serves HTTP on port **8082**.

---

## What's in this project

```
kafka-phase3/
├── pom.xml
└── src/main/
    ├── java/com/learn/kafka/
    │   ├── Phase3Application.java
    │   ├── TopicConfig.java              # creates 'orders-json' (3 partitions)
    │   ├── OrderProducer.java            # KafkaTemplate<String, OrderEvent>
    │   ├── OrderListener.java            # @KafkaListener(OrderEvent)
    │   ├── OrderController.java          # POST /orders  (JSON body)
    │   └── model/
    │       ├── OrderEvent.java           # the event: nested customer + item list
    │       ├── Customer.java             # nested object
    │       └── LineItem.java             # list element
    └── resources/
        └── application.yml               # JSON serializer/deserializer config
```

**Prerequisites:** JDK 17+ and Maven.

---

## Step 1 — Run

```bash
mvn spring-boot:run
```

---

## Step 2 — POST a real order (nested JSON)

```bash
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{
        "orderId": "ORD-1001",
        "customer": { "id": "cust-7", "name": "Asha Rao", "email": "asha@example.com" },
        "items": [
          { "sku": "BOOK-42", "quantity": 2, "price": 12.50 },
          { "sku": "PEN-01",  "quantity": 5, "price": 1.20 }
        ],
        "total": 31.00,
        "createdAt": "2026-07-19T10:15:00Z"
      }'
```

In the app log you'll see the producer send it, then the listener rebuild the
**object** and walk its nested fields:

```
Sent order ORD-1001 (key=cust-7) -> partition X, offset Y
Received order ORD-1001 from Asha Rao <asha@example.com> — total $31.0
    line: BOOK-42 x2 @ $12.5
    line: PEN-01 x5 @ $1.2
```

The nested `Customer` and the `items` list came back intact — that's JSON
serialization round-tripping a real object graph through Kafka.

---

## How it works — the two things that make typed JSON click

**1. Type headers.** The `JsonSerializer` (set in `application.yml`) doesn't just
write JSON bytes — it also adds a Kafka header, `__TypeId__`, containing the fully
qualified class name (`com.learn.kafka.model.OrderEvent`). On the other side, the
`JsonDeserializer` reads that header and knows exactly which class to build. That's
why the listener can just declare an `OrderEvent` parameter and it works, with no
manual type wiring.

**2. Trusted packages.** Rebuilding an arbitrary class named in a message header
is a security risk (a malicious producer could name a dangerous class). So
`JsonDeserializer` refuses unless the class is in a package you listed under
`spring.json.trusted.packages`. We trust only `com.learn.kafka.model`. If you ever
see a *"not in trusted packages"* error, this is the setting to check. **Never use
`"*"` in production.**

> Peek at the raw bytes: open the Kafka UI (http://localhost:8080), find the
> `orders-json` topic, and look at a message. The value is plain JSON, and you'll
> see the `__TypeId__` header alongside it.

---

## Experiments

- **Break trust on purpose:** change `spring.json.trusted.packages` to
  `com.learn.kafka.wrong`, restart, POST an order, and watch the listener fail to
  deserialize. Set it back. Now you'll recognize that error instantly.
- **Consume without type headers:** a common real setup is the producer *not*
  sending type headers and the consumer being told a fixed default type via
  `spring.json.value.default.type: com.learn.kafka.model.OrderEvent`. Try it — it
  teaches you the two ways a consumer can learn the target type.
- **Evolve the schema (the cliffhanger):** add a new field to `OrderEvent` (e.g.
  `String note`) on the producer side but imagine an *old* consumer that doesn't
  know about it. JSON tolerates this loosely (unknown fields are usually ignored),
  but nothing *enforces* compatibility. That gap is exactly what the Schema
  Registry fills.

---

## Concept: Schema Registry & Avro (know this, no code required yet)

With plain JSON, the "contract" between producer and consumer is informal — if a
producer renames `total` to `amount`, consumers silently break at runtime. On a
real team with many services, that's dangerous.

The industry answer is a **Schema Registry** (Confluent's is the common one) paired
with a compact binary format, usually **Avro** (sometimes Protobuf):

- Every message's schema is registered centrally and referenced by id, so messages
  on the wire are small (no field names repeated in every record).
- The registry enforces **compatibility rules** — e.g. "you may add an optional
  field, but you may not remove a required one" — and *rejects* a producer whose
  new schema would break existing consumers.
- This lets many teams evolve shared event formats safely over time.

You don't need to implement this to be productive, but you should be able to say
*why* it exists: **JSON gives you types; a Schema Registry gives you an enforced,
evolvable contract.** Some teams stay on JSON for simplicity; others adopt
Avro + Registry once multiple services depend on the same events.

---

## Phase 3 checklist

- [ ] App runs; POST /orders returns 202
- [ ] Listener logs the rebuilt object including nested customer + item lines
- [ ] Saw the JSON value and `__TypeId__` header in the Kafka UI
- [ ] Understand type headers (how the consumer learns the class)
- [ ] Understand trusted packages (and the error when a class isn't trusted)
- [ ] Can explain in one sentence why a Schema Registry exists

When these are ticked, say **"start Phase 4"** — reliability: delivery semantics,
manual acks, retries, and dead-letter topics. That's the phase that separates
people who *used* Kafka from people who *understand* it.
