package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionalKafkaProducer {

    public static void main(String[] args) {
        // Kafka producer configuration settings
        String topicName = "retention-1-min-3-part";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Required for transactional producer
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer-1");

        // Create the Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Initialize the transactional context
        producer.initTransactions();

        try {
            // Begin the transaction
            producer.beginTransaction();

            // Send messages within a transaction
            producer.send(new ProducerRecord<>(topicName, "key1", "value1"));
            producer.send(new ProducerRecord<>(topicName, "key2", "value2"));
            producer.send(new ProducerRecord<>(topicName, "key2", "value3"));
            producer.send(new ProducerRecord<>(topicName, "key2", "value4"));
            producer.send(new ProducerRecord<>(topicName, "key2", "value5"));

            // Commit the transaction if everything is successful
            producer.commitTransaction();
            System.out.println("Transaction committed successfully.");
        } catch (Exception e) {
            // If any exception occurs, abort the transaction
            producer.abortTransaction();
            System.err.println("Transaction aborted due to an error: " + e.getMessage());
        } finally {
            // Close the producer
            producer.close();
        }
    }
}
