package com.kafka.demo;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka_basics.User;


import java.util.Properties;

public class UserProducer {
    private static Logger log = LoggerFactory.getLogger(UserProducer.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8100");

        String topicName = "user";

        KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<>(properties);
        int i = 0;
        while (i < 100) {

            GenericRecord record = new GenericRecordBuilder(User.SCHEMA$)
                    .set("id", i)
                    .set("name", "hello world " + i)
                    .set("email", "deexith "+ i)
                    .build();
            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topicName, "ID -" + i, record);
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                log.info("Topic : " + metadata.topic() + "\n");
                log.info("Offset : " + metadata.offset() + "\n");
                log.info("Partition : " + metadata.partition() + "\n");
            });

            kafkaProducer.flush();
            i++;
        }



    }
}
