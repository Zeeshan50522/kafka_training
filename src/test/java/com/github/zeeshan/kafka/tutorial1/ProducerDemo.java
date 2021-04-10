package com.github.zeeshan.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) { 
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // creating kafka producer
        Producer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        // kafka producer record
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","Hello world");

        kafkaProducer.send(record);

        // flush producer data
        kafkaProducer.flush();

        //close producer
        kafkaProducer.close();

    }
}
