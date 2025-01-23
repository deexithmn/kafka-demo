package com.wiki.media;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandleImpl implements EventHandler {

    private String topic;
    private KafkaProducer<String, String> kafkaProducer;

    private static final Logger log = LoggerFactory.getLogger(EventHandleImpl.class);

    EventHandleImpl(String topic, KafkaProducer<String, String> kafkaProducer) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;

    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        this.kafkaProducer.send(new ProducerRecord<>(this.topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
