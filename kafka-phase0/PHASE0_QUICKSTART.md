# Phase 0 — Kafka Quickstart (KRaft mode)

Goal of this phase: get a real Kafka broker running, learn the vocabulary, and
watch a message you type in one terminal appear in another. No Java yet.

**Prerequisite:** Docker Desktop (or Docker Engine) installed and running.
Check with:

```bash
docker --version
```

---

## Step 1 — Start Kafka

Put `docker-compose.yml` in a folder, open a terminal there, and run:

```bash
docker compose up -d
```

The first run downloads the images (a minute or two). Confirm both containers
are healthy:

```bash
docker compose ps
```

You should see `kafka` and `kafka-ui` running. Open the UI in your browser:

    http://localhost:8080

Keep that tab open — you'll literally watch topics and messages appear in it as
you run the commands below.

> A note on the commands: the Kafka CLI tools live *inside* the container, so
> every command is prefixed with `docker exec -it kafka ...`. That just means
> "run this command inside the kafka container."

---

## Step 2 — Create your first topic

A **topic** is a named stream of messages. We'll create one called `greetings`
with 3 partitions.

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic greetings --partitions 3 --replication-factor 1
```

List topics to confirm it exists:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Describe it to see its partitions:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic greetings
```

Notice the output shows Partition 0, 1, 2 — each is a separate ordered log.
Refresh the UI and you'll see `greetings` there too.

---

## Step 3 — Consume (open this terminal FIRST, leave it running)

Open a **second terminal**. Start a consumer that waits for messages:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic greetings --from-beginning
```

This will just sit there waiting. That's correct — leave it running.

---

## Step 4 — Produce (in your first terminal)

Back in your **first terminal**, start a producer:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic greetings
```

You'll get a `>` prompt. Type a line and press Enter, a few times:

```
> hello kafka
> my first event
> backend developer here
```

Switch to your second terminal — each line appears there almost instantly.
**That's the whole magic of Kafka:** a producer appended records to a log, and a
consumer read them back. Press `Ctrl+C` to exit the producer when done.

---

## Step 5 — See keys decide partitions (optional, but do it)

Stop your plain consumer (`Ctrl+C`) and restart it so it prints which partition
each message came from, plus its key:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic greetings --from-beginning \
  --property print.partition=true --property print.key=true \
  --property key.separator=" | "
```

Now produce messages **with keys** (anything before the `:` is the key):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic greetings \
  --property parse.key=true --property key.separator=:
```

```
> user-A:first
> user-B:hello
> user-A:second
> user-A:third
```

Watch the consumer: every `user-A` message lands on the **same partition**,
while `user-B` may land on a different one. This is the single most important
behavior in Kafka — **the key determines the partition, and order is guaranteed
within a partition.** You'll rely on this constantly later.

---

## The vocabulary (be able to say each in one sentence)

- **Broker** — a single Kafka server. Our cluster has one.
- **Topic** — a named stream of messages (e.g. `greetings`).
- **Partition** — an ordered, append-only log; a topic is split into several.
- **Offset** — a message's position number within a partition; only moves forward.
- **Record** — one message: an optional **key**, a **value**, and a timestamp.
- **Producer** — writes records to a topic.
- **Consumer** — reads records from a topic.
- **Consumer group** — a set of consumers that share the work of a topic; Kafka
  splits partitions among them (you'll explore this in Phase 1).

---

## When you're done

Stop the cluster (keeps your data):

```bash
docker compose down
```

Or stop and wipe everything clean:

```bash
docker compose down -v
```

---

## Phase 0 checklist

- [ ] `docker compose up -d` and both containers running
- [ ] Opened the UI at http://localhost:8080
- [ ] Created the `greetings` topic with 3 partitions
- [ ] Sent messages with a producer and saw them in a consumer
- [ ] Saw the same key always land on the same partition
- [ ] Can explain topic, partition, offset, producer, consumer in one sentence each

When those boxes are ticked, you're ready for **Phase 1 — the raw Java client**.
Just say the word and I'll set it up.
