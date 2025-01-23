package com.kafka.demo;

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

public class ConsumerDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "test-group-1");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        Thread mainThread = Thread.currentThread();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.error("Shutdown hook invoked");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }));


        kafkaConsumer.subscribe(Arrays.asList("topic-new-123"));

        try {

            while (true) {
                log.error("Polling");
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.error("Key: " + consumerRecord.key() + " - Value: " + consumerRecord.value());
                    log.error("Partitions: " + consumerRecord.partition() + " - offset: " + consumerRecord.offset());
                }
            }
        } catch (WakeupException wakeupException) {
            log.error("Wake Up execution called");
            log.error(String.valueOf(wakeupException));
        } finally {
            log.error("Closing the consumer gracefully");
            kafkaConsumer.close();
        }


    }
}
