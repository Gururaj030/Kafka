package com.learn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * PHASE 1 — Consumer with MANUAL offset commits.
 *
 * Run this in TWO terminals at once (same group id) to watch Kafka split the
 * partitions between them — and rebalance when you kill one. See PHASE1_GUIDE.md.
 *
 * The group id can be overridden as the first argument; it defaults to
 * "phase1-group". Every consumer sharing a group id shares the work.
 */
public class ConsumerApp {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "orders";

    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : "phase1-group";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // If this group has never read before, start at the beginning of the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Turn OFF auto-commit so WE decide when an offset is committed.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Allow a clean shutdown on Ctrl+C: wakeup() makes poll() throw WakeupException.
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down consumer...");
            consumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException ignored) {
            }
        }));

        System.out.println("Consumer started. group.id=" + groupId + "  (Ctrl+C to stop)\n");

        try {
            consumer.subscribe(List.of(TOPIC));

            while (true) {
                // Ask Kafka for any new records, waiting up to 1 second.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition %d | offset %-4d | key=%-11s | value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }

                // Manually commit AFTER we've processed the batch. This is
                // "at-least-once" delivery: if we crash before committing, we'll
                // reprocess these records next time rather than lose them.
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // Normal shutdown path — ignore.
        } finally {
            consumer.close();
            System.out.println("Consumer closed cleanly.");
        }
    }
}
