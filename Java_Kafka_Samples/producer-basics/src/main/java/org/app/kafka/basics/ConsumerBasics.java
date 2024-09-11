package org.app.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerBasics {

    public static final Logger log = LoggerFactory.getLogger(ConsumerBasics.class.getSimpleName());

    public static void main(String[] args) {

        String topic = "demo-topic";

        KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer();
        consumer.subscribe(List.of(topic));


        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord: records) {
                log.info("Topic: {} | Partition: {} | Message: {} | Offset: {}",
                        consumerRecord.topic(), consumerRecord.partition(), consumerRecord.value(), consumerRecord.offset());
            }
        }
    }

    private static KafkaConsumer<String, String> getStringStringKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092,localhost:29093,localhost:29094");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "my-java-application");
        properties.setProperty("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(properties);
    }
}
