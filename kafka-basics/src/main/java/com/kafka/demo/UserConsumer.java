package com.kafka.demo;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class UserConsumer {
    private final static Logger log = LoggerFactory.getLogger(UserConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8100");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        properties.setProperty("group.id", "consumer-3");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("specific.avro.reader", "true");

        String topic = "user";

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);



        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {

            consumer.subscribe(Arrays.asList(topic));

            while(true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    com.kafka_basics.User user = (com.kafka_basics.User) record.value();
                    log.info("=========================================");
                    log.info("Key: " + record.key());
                    log.info("Name: " + user.getName());
                    log.info("Email: " + user.getEmail());
                    log.info("=========================================");
                }
            }
        } catch (WakeupException wakeupException) {
            log.error("Wakeup Execution is called");
        } finally {
            consumer.close();
        }


    }

}
