# Phase 1 — The Raw Java Client

Goal: write producers and consumers with the plain `kafka-clients` library — no
Spring yet. By the end you'll understand producer configs, how a **key picks the
partition**, manual **offset commits**, and consumer-group **rebalancing**. These
are the mechanics Spring will hide later, so learning them now pays off for the
rest of the course.

Your Kafka from Phase 0 should be running (`docker compose up -d`). Everything
here talks to it at `localhost:9092`.

---

## What's in this project

```
kafka-phase1/
├── pom.xml                         # Maven build + kafka-clients dependency
└── src/main/
    ├── java/com/learn/kafka/
    │   ├── ProducerApp.java        # sends 100 keyed messages, prints partition/offset
    │   └── ConsumerApp.java        # manual-commit consumer (run 2 copies to see rebalance)
    └── resources/
        └── simplelogger.properties # keeps Kafka's logs quiet
```

**Prerequisites:** JDK 17+ and Maven. Check with `java -version` and `mvn -version`.

---

## Step 1 — Build

From inside the `kafka-phase1` folder:

```bash
mvn clean compile
```

First run downloads dependencies. A `BUILD SUCCESS` means you're ready.

---

## Step 2 — Run the producer

```bash
mvn exec:java -Dexec.mainClass=com.learn.kafka.ProducerApp
```

It creates the `orders` topic with 3 partitions (if needed), sends 100 messages,
and prints where each landed:

```
sent key=customer-0  -> partition 2, offset 0
sent key=customer-1  -> partition 0, offset 0
sent key=customer-2  -> partition 2, offset 1
...
```

**Look closely:** every `customer-0` message goes to the *same* partition, every
`customer-1` to the same partition, and so on. That's the core rule — Kafka hashes
the key to choose a partition, so a given key's messages stay ordered on one
partition. (Your exact partition numbers may differ from mine; what matters is
that each key is *consistent*.)

---

## Step 3 — Run the consumer

In another terminal:

```bash
mvn exec:java -Dexec.mainClass=com.learn.kafka.ConsumerApp
```

It reads from the beginning and prints every record with its partition, offset,
key, and value, then **manually commits** the offsets. Press `Ctrl+C` for a clean
shutdown.

Why manual commit matters: we commit *after* processing a batch. If the app
crashed mid-batch, those records aren't committed, so on restart Kafka redelivers
them — nothing is silently lost. This is **at-least-once** delivery, the most
common choice in real systems.

---

## Step 4 — The main event: watch a rebalance

This is the experiment to really understand consumer groups.

**1.** Open **two terminals**. In each, start a consumer *with the same group id*
(the default, `phase1-group`):

Terminal A:
```bash
mvn exec:java -Dexec.mainClass=com.learn.kafka.ConsumerApp
```
Terminal B:
```bash
mvn exec:java -Dexec.mainClass=com.learn.kafka.ConsumerApp
```

Because both share one group, Kafka **divides the 3 partitions between them** —
e.g. one consumer gets partitions 0 and 1, the other gets partition 2. They are
splitting the work, not each getting everything.

**2.** In a third terminal, run the producer again to generate traffic:
```bash
mvn exec:java -Dexec.mainClass=com.learn.kafka.ProducerApp
```
Watch the messages split across the two consumer windows by partition.

**3.** Now **kill one consumer** (`Ctrl+C` in Terminal A). Watch Terminal B: after
a moment Kafka **rebalances** and reassigns *all 3 partitions* to the survivor.
Re-run the producer — the lone consumer now handles everything.

**4.** Restart the second consumer and watch the partitions get shared again.

> Key insight: a consumer group scales by partition. With 3 partitions, at most 3
> consumers in a group do useful work at once — a 4th would sit idle with nothing
> assigned. This partition/consumer relationship comes back in Phase 5.

---

## Experiments to cement it (highly encouraged)

- **Break durability on purpose:** in `ProducerApp`, change `acks` from `"all"`
  to `"0"` and re-run. The producer no longer waits for acknowledgment — faster,
  but a broker hiccup could silently drop messages. Set it back to `"all"` after.
- **Change the group id:** run a consumer with a different group:
  ```bash
  mvn exec:java -Dexec.mainClass=com.learn.kafka.ConsumerApp -Dexec.args="other-group"
  ```
  A *new* group reads the whole topic from the beginning again, independently of
  `phase1-group`. Groups are isolated from each other — this is how two different
  services can each consume the same events.
- **Use fewer keys:** change the producer to a single fixed key and watch every
  message pile onto one partition (perfect ordering, zero parallelism — the
  central trade-off).

---

## Phase 1 checklist

- [ ] `mvn clean compile` succeeds
- [ ] Producer sends 100 messages; same key always maps to the same partition
- [ ] Consumer reads all messages and commits manually
- [ ] Ran two consumers in one group and saw partitions split between them
- [ ] Killed one consumer and watched the rebalance move partitions to the other
- [ ] Can explain what `acks=all` does and why manual commit gives at-least-once

When these are ticked, you're ready for **Phase 2 — Spring Boot fundamentals**,
where `KafkaTemplate` and `@KafkaListener` replace all this boilerplate. Just say
"start Phase 2".
