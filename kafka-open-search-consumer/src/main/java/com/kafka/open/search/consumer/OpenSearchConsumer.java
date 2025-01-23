package com.kafka.open.search.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        String topic = "wikimedia.recentChanges";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


        try (openSearchClient; consumer) {
            boolean doesIndexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!doesIndexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia-new");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index created successfully");
            } else {
                log.info("wikimedia index already exist");
            }

            consumer.subscribe(Collections.singleton(topic)); // Subscribe to the topic
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                log.info("Polling from topic wikimedia.recentChanges");

                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.valueOf(record.offset()));

                    try {
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia-new")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        // Index the record into OpenSearch
                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("Added the index: " + indexResponse.getId());

                    } catch (WakeupException e) {
                        log.info("Consumer is starting to shut down");
                        break; // Exit the loop on shutdown
                    } catch (Exception e) {
                        log.error("Unexpected exception in the consumer", e);
                    }
                }
            }
        } catch (WakeupException | IOException e) {
            log.error("Wakeup exception or IO exception called", e);
        } finally {
            log.info("Consumer and OpenSearch client have been closed.");
            consumer.close();
            openSearchClient.close();
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        return properties;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("admin", "admin"));

        //Create a client.
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
