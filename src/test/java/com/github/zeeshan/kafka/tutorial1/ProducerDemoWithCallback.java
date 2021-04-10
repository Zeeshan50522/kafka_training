package com.github.zeeshan.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer<String,String> KafkaProducer = new KafkaProducer<>(properties);

        for (int i=0 ; i<10 ; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world");

            KafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received metadata \n" +
                            "Topic: " + recordMetadata.topic() +
                            "Partition: " + recordMetadata.partition() +
                            "Offset: " + recordMetadata.offset() +
                            "TimeStamp: " + recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }
        // flush producer data
        KafkaProducer.flush();

        //close producer
        KafkaProducer.close();
    }
}
