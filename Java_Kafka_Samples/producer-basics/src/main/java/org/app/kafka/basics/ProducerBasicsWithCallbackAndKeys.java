package org.app.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerBasicsWithCallbackAndKeys {
    public static final Logger log = LoggerFactory.getLogger(ProducerBasicsWithCallbackAndKeys.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");

        // Serializers
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Initialize the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j=1; j<=2; j++) {
            log.info("Processing batch {} of 2", j);
            for (int i = 1; i <= 10; i++) {

                String key = "id_" + i;
                String topic = "demo-topic";
                String message = "hello-from-java" + i *j;

                // Make message
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);

                // Send the message
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info("Key:{}", key);
                        log.info("Partition: {}", recordMetadata.partition());
                    } else {
                        log.error(e.getMessage());
                    }

                });

            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Synchronously send and commit the record offset
        producer.flush();

        // Close the connection
        producer.close();
    }
}