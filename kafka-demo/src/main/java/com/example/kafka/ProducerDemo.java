package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // Kafka topic name
        String topicName = "test-topic";

        // Producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send 5 messages
        for (int i = 1; i <= 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "Key-" + i, "Message-" + i);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent -> Topic:%s Partition:%d Offset:%d Key:%s Value:%s%n",
                            metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // Close producer
        producer.close();
    }
}