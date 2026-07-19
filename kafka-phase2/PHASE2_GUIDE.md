# Phase 2 — Spring Boot Fundamentals

Goal: replace all of Phase 1's boilerplate with Spring. You'll produce with
`KafkaTemplate`, consume with `@KafkaListener`, configure everything in
`application.yml`, and wire up the pattern you'll use most as a backend
developer: **an HTTP request comes in, a Kafka event goes out.**

Keep your Phase 0 Kafka running (`docker compose up -d`). This app talks to it at
`localhost:9092` and serves HTTP on port **8081** (8080 is the Kafka UI).

---

## What's in this project

```
kafka-phase2/
├── pom.xml                              # Spring Boot 3.5 + spring-kafka
└── src/main/
    ├── java/com/learn/kafka/
    │   ├── Phase2Application.java        # @SpringBootApplication entry point
    │   ├── TopicConfig.java              # creates the topic (3 partitions) on startup
    │   ├── EventProducer.java            # KafkaTemplate — produces
    │   ├── EventListener.java            # @KafkaListener — consumes
    │   └── EventController.java          # POST /events — the REST bridge
    └── resources/
        └── application.yml               # all Kafka config lives here
```

Notice what's **missing** compared to Phase 1: no `Properties`, no manual
`KafkaProducer`/`KafkaConsumer`, no poll loop, no commit calls. Spring builds all
of that from `application.yml`.

**Prerequisites:** JDK 17+ and Maven.

---

## Step 1 — Run the app

From inside the `kafka-phase2` folder:

```bash
mvn spring-boot:run
```

On startup you'll see Spring boot up, create the `app-events` topic, and the
listener join `phase2-group`. Leave it running — the logs here are where you'll
watch messages being consumed.

---

## Step 2 — Publish an event over HTTP

In a second terminal, POST a message. The `key` is a query param, the body is the
message:

```bash
curl -X POST "http://localhost:8081/events?key=customer-1" \
     -H "Content-Type: text/plain" -d "hello from REST"
```

You'll get back `Published (key=customer-1): hello from REST`.

Now look at the **app's log window**. You'll see two lines, and this is the whole
point of the phase:

```
Sent key=customer-1 -> partition X, offset Y        <- the producer (KafkaTemplate)
Received key=customer-1 | value='hello from REST' ...  <- the listener (@KafkaListener)
```

One `curl` → an event was produced to Kafka → your listener consumed it. That
round trip is the backbone of event-driven backends.

---

## Step 3 — Prove the key still drives the partition

Fire several messages with different keys:

```bash
for i in 1 2 3 4 5; do
  curl -s -X POST "http://localhost:8081/events?key=customer-$((i % 2))" \
       -H "Content-Type: text/plain" -d "message $i" > /dev/null
done
```

Watch the log: every `customer-0` lands on one partition and every `customer-1`
on another — the exact rule you learned in Phase 1, unchanged. Spring didn't
replace Kafka's mechanics; it just removed the boilerplate around them. You can
also open the Kafka UI at http://localhost:8080 and browse the `app-events`
topic to see the messages sitting in their partitions.

---

## How the pieces connect

- **application.yml** holds bootstrap servers, serializers, group id — the same
  settings you set in code in Phase 1, now declarative.
- **EventProducer** injects the `KafkaTemplate` Spring auto-built and calls
  `send(topic, key, message)`. The `.whenComplete(...)` callback logs the
  partition/offset, mirroring Phase 1's producer callback.
- **EventListener** is just a method with `@KafkaListener`. Spring runs the poll
  loop and commits offsets for you in a background thread.
- **EventController** turns an HTTP POST into a producer call.
- **TopicConfig** declares a `NewTopic` bean so the topic is created with 3
  partitions on startup.

---

## Experiments (recommended)

- **Scale consumers in-process:** add `concurrency = "3"` to the `@KafkaListener`
  annotation (`@KafkaListener(topics = "${app.topic}", concurrency = "3")`),
  restart, and watch Spring run three consumer threads splitting the 3 partitions
  — the Phase 1 rebalance idea, now inside one app. (This is a preview of Phase 5.)
- **Send JSON as plain text:** POST a JSON string and notice it arrives as a raw
  string. That itch — wanting real typed objects instead of strings — is exactly
  what Phase 3 solves.
- **Add a second listener method** with a *different* `groupId` attribute and see
  both receive every message independently (two groups = two independent readers).

---

## Phase 2 checklist

- [ ] `mvn spring-boot:run` starts the app cleanly
- [ ] `POST /events` returns 202 and you see Sent + Received in the logs
- [ ] Same key consistently maps to the same partition
- [ ] Browsed the topic in the Kafka UI
- [ ] Understand which job each class does (producer / listener / controller / config)
- [ ] Can explain how application.yml replaces Phase 1's Properties setup

When these are ticked, say **"start Phase 3"** and we'll move from String messages
to real typed JSON objects (`OrderEvent` POJOs), trusted packages, and a first
look at the Schema Registry.
