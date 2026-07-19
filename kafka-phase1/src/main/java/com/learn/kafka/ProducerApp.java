package com.learn.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * PHASE 1 — Producer.
 *
 * Sends 100 messages to the "orders" topic. Each message has a KEY, and the key
 * decides which partition it lands on. The callback prints the partition and
 * offset Kafka assigned, so you can SEE that same-key messages share a partition.
 */
public class ProducerApp {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "orders";
    private static final int PARTITIONS = 3;

    public static void main(String[] args) throws Exception {
        // Make sure the topic exists with 3 partitions (so the key->partition
        // behavior is visible). Safe to run repeatedly.
        ensureTopic();

        // --- Producer configuration ---
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all -> wait until the leader (and in-sync replicas) store the record.
        // This is the safe, durable setting. Try changing it to "0" later and observe.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // try-with-resources auto-closes (and flushes) the producer at the end.
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < 100; i++) {
                // 5 distinct customers -> 5 distinct keys.
                String key = "customer-" + (i % 5);
                String value = "order #" + i + " for " + key;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, key, value);

                // Asynchronous send with a callback. The callback runs once Kafka
                // acknowledges the record and tells us WHERE it landed.
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("sent key=%-11s -> partition %d, offset %d%n",
                                key, metadata.partition(), metadata.offset());
                    }
                });
            }

            // Block until all buffered records are actually sent.
            producer.flush();
            System.out.println("\nDone. Sent 100 messages to topic '" + TOPIC + "'.");
            System.out.println("Notice: every 'customer-N' always maps to the SAME partition.");
        }
    }

    /** Create the topic with 3 partitions if it doesn't already exist. */
    private static void ensureTopic() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP);
        try (Admin admin = Admin.create(adminProps)) {
            boolean exists = admin.listTopics().names().get().contains(TOPIC);
            if (!exists) {
                NewTopic topic = new NewTopic(TOPIC, PARTITIONS, (short) 1);
                admin.createTopics(List.of(topic)).all().get();
                System.out.println("Created topic '" + TOPIC + "' with " + PARTITIONS + " partitions.\n");
            } else {
                System.out.println("Topic '" + TOPIC + "' already exists.\n");
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Could not ensure topic exists", e);
        }
    }
}
