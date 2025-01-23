package com.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Hello from Kafka producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("partitions", "3");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> newProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> newRecord = new ProducerRecord<>("demo_java_new", "hello world 1235");
        newProducer.send(newRecord, ((recordMetadata, e) -> {
            if(e == null) {
                log.error("Received new metadata \n" +
                         "Topic: " + recordMetadata.topic() +", \n" +
                         "Partition: " + recordMetadata.partition() +", \n" +
                         "Offset: " + recordMetadata.offset() +", \n" +
                        "Timestamp: " + recordMetadata.timestamp()
                );
            } else {
                log.error("Error while producing");
            }
        }));

        newProducer.flush();

//        newProducer.close();


        Properties newProperties = new Properties();
        newProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        newProperties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        log.info("===================================");

        try(AdminClient client = AdminClient.create(newProperties)) {
            String topicName = "topic-new-123";
            int partitions = 6;
            short replitcationFactor = 1;
            NewTopic newTopic = new NewTopic(topicName, partitions, replitcationFactor);
            log.error(newTopic + "created");

            for(int i=0; i < 15; i++) {
                String key = "ID -" + i;
                String value = "Hello Test -" + i;
                ProducerRecord<String, String> newRecord1 = new ProducerRecord<>(newTopic.name(), key, value);

                newProducer.send(newRecord1, ((recordMetadata, e) -> {
                    if(e == null) {
                        log.error("Key:" + key + " -- " + "Partition: " + recordMetadata.partition() +", \n" +
                                "Offset: " + recordMetadata.offset() +", \n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        log.error("Error while producing");
                    }
                }));

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
