package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMessageProducer {

    public static void main(String[] args) {
        // Define the Kafka broker and topic details
        String bootstrapServers = "10.128.0.121:9092"; // Change to your Kafka broker address
        String topic = "retention-1-min-3-part"; // Change to your Kafka topic

        // Set up the producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a message to send to the topic
        String key = "my-key";
        String value = "Hello, Kafka!";

        // Send the message
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        try {
            // Send the record and print the metadata
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent message to topic: %s, partition: %d, offset: %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error while producing message: " + e.getMessage());
        } finally {
            // Close the producer
            producer.close();
        }
    }
}