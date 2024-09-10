package org.app.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerBasics {
    public static final Logger log = LoggerFactory.getLogger(ProducerBasics.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");

        // Serializers
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Initialize the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j=1; j<=10; j++) {
            for (int i = 1; i <= 30; i++) {
                log.info("Processing record : {}", i * j);
                // Make message
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-topic", "hello-world-from-java" + i);

                // Send the message
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info("Topic:{}", recordMetadata.topic());
                        log.info("Partition:{}", recordMetadata.partition());
                        log.info("Offset:{}", recordMetadata.offset());
                        log.info("Timestamp:{}", recordMetadata.timestamp());
                    } else {
                        log.error(e.getMessage());
                    }

                });

                // Synchronously send and commit the record offset
                producer.flush();

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }



        // Close the connection
        producer.close();
    }
}