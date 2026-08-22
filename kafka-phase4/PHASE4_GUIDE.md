# Phase 4 — Reliability: What Matters in Production

This is the phase that separates people who *used* Kafka from people who
*understand* it. You'll make the producer safe against duplicates, then build the
single most important consumer-side pattern in real systems: **retries with a
dead-letter topic** so one bad message can't take down your pipeline.

Keep Phase 0 Kafka running. This app serves HTTP on port **8083**.

---

## First, the mental model: delivery semantics

Every messaging system makes a promise about how many times a message is
delivered. There are three:

- **At-most-once** — deliver, then record progress immediately. If you crash
  mid-processing, the message is *lost*. Fast, lossy. (Auto-commit before
  processing behaves like this.)
- **At-least-once** — process first, record progress after. If you crash before
  recording, you'll *reprocess* the message on restart. Nothing is lost, but you
  may see duplicates. **This is the common default and what we use here.**
- **Exactly-once** — no loss and no duplicates. Kafka supports it via idempotent
  producers + transactions, but it's more complex and has overhead. Reach for it
  only when duplicates are genuinely unacceptable (e.g. financial ledgers).

The practical recipe most teams use: **at-least-once delivery + idempotent
processing** (make handling a message twice harmless). That's cheaper and simpler
than full exactly-once and covers the vast majority of cases.

---

## What's in this project

```
kafka-phase4/
├── pom.xml
└── src/main/
    ├── java/com/learn/kafka/
    │   ├── Phase4Application.java
    │   ├── RetryConfig.java          # @EnableKafkaRetryTopic — turns on retries
    │   ├── TopicConfig.java          # main topic only (retry/dlt auto-created)
    │   ├── OrderProducer.java        # idempotent producer
    │   ├── OrderListener.java        # @RetryableTopic + @DltHandler  <-- the core
    │   ├── OrderController.java      # POST /orders
    │   └── model/ (OrderEvent, Customer, LineItem)
    └── resources/application.yml     # acks=all, enable.idempotence=true, JSON serdes
```

---

## Step 1 — Run

```bash
mvn spring-boot:run
```

On startup Spring creates the main topic plus a set of **retry topics** and a
**dead-letter topic** automatically. Watch the log — you'll see topics like
`orders-v4-retry-0`, `orders-v4-retry-1`, `orders-v4-retry-2`, and `orders-v4-dlt`
get created. You can also see them all in the Kafka UI (http://localhost:8080).

---

## Step 2 — A healthy order (processed once)

```bash
curl -X POST http://localhost:8083/orders \
  -H "Content-Type: application/json" \
  -d '{
        "orderId": "ORD-GOOD",
        "customer": { "id": "cust-1", "name": "Asha", "email": "asha@example.com" },
        "items": [ { "sku": "BOOK-42", "quantity": 1, "price": 10.00 } ],
        "total": 10.00,
        "createdAt": "2026-07-20T09:00:00Z"
      }'
```

Log:
```
Sent order ORD-GOOD -> partition X, offset Y
Processing order ORD-GOOD (from topic 'orders-v4')
  OK — order ORD-GOOD processed successfully. total=$10.0
```

Processed exactly once. No drama.

---

## Step 3 — A poison order (retries, then dead-letters)

Our listener throws on any order with a **negative total**. Send one:

```bash
curl -X POST http://localhost:8083/orders \
  -H "Content-Type: application/json" \
  -d '{
        "orderId": "ORD-BAD",
        "customer": { "id": "cust-2", "name": "Ben", "email": "ben@example.com" },
        "items": [ { "sku": "PEN-01", "quantity": 1, "price": -5.00 } ],
        "total": -5.00,
        "createdAt": "2026-07-20T09:05:00Z"
      }'
```

Now watch the log tell a story over a few seconds:

```
Processing order ORD-BAD (from topic 'orders-v4')          <- attempt 1 (main topic)
Processing order ORD-BAD (from topic 'orders-v4-retry-0')  <- attempt 2, after ~1s
Processing order ORD-BAD (from topic 'orders-v4-retry-1')  <- attempt 3, after ~2s
Processing order ORD-BAD (from topic 'orders-v4-retry-2')  <- attempt 4, after ~4s
DEAD-LETTER: order ORD-BAD gave up after all retries and landed on 'orders-v4-dlt'. Needs manual attention.
```

That's the whole pattern. The message was retried with **exponential backoff**
(1s → 2s → 4s), and when it kept failing it was moved to the **dead-letter topic**
instead of being lost or blocking the queue forever. Open the Kafka UI and look at
`orders-v4-dlt` — the failed order is sitting there for inspection.

**Why "non-blocking" matters:** the retries happen on *separate* topics, so a
single poison message doesn't stall the healthy messages behind it on the main
topic. Send a good order right after a bad one and the good one sails through
immediately while the bad one is off doing its retry dance.

---

## The idempotent producer (Step 3 already used it)

Look at `application.yml`:

```yaml
producer:
  acks: all
  properties:
    enable.idempotence: true
```

With idempotence on, if the producer client retries a send after a network
glitch, the broker recognizes the duplicate (via a producer id + sequence number)
and stores the record only **once**. Without it, a retried send could append the
same record twice. `acks=all` is required for this guarantee.

**Experiment to see the difference conceptually:** set `enable.idempotence: false`
and `acks: 0`, restart, and understand that you've now traded safety for speed —
the producer no longer waits for acknowledgment and retried sends could duplicate
or drop. Put it back to `true`/`all` afterward. (Modern clients default to
idempotent; we set it explicitly so you *know* it's on.)

---

## Manual acknowledgment (know this pattern)

Our `@RetryableTopic` listener lets Spring manage offset commits for you. The
other reliability lever you should recognize is **manual acks**, where *you*
decide when a message is marked done. It's how you guarantee at-least-once in a
plain listener: commit only after your processing (e.g. a DB write) succeeds.

To use it, set the container ack mode to MANUAL and take an `Acknowledgment`
parameter:

```java
// application.yml:  spring.kafka.listener.ack-mode: MANUAL

@KafkaListener(topics = "some-topic")
public void handle(OrderEvent order, Acknowledgment ack) {
    saveToDatabase(order);   // do the real work first
    ack.acknowledge();       // ONLY commit the offset after success
}
```

If the app crashes before `ack.acknowledge()`, the offset isn't committed, so on
restart Kafka redelivers the message — nothing is lost. That's at-least-once by
construction. (We keep it out of the runnable code here because it doesn't mix
cleanly with `@RetryableTopic`, which handles acks itself — but this is the
snippet to reach for in a non-retry listener.)

---

## Phase 4 checklist

- [ ] App starts and auto-creates the retry + dead-letter topics
- [ ] A good order is processed once
- [ ] A poison order retries with backoff, then lands in `orders-v4-dlt`
- [ ] Saw the dead-lettered message in the Kafka UI
- [ ] Can explain at-most / at-least / exactly-once in a sentence each
- [ ] Understand what `enable.idempotence=true` protects against
- [ ] Understand when/why to use manual acknowledgment

When these are ticked, say **"start Phase 5"** — concurrency, partitions, and
performance: scaling consumers with `concurrency`, consumer lag, and the
ordering-vs-parallelism trade-off.
