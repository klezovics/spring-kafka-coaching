package com.klezovich.kafkacoaching.basic;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
//Based on: https://www.baeldung.com/spring-boot-kafka-testing
@Slf4j
class EmbeddedKafkaIntegrationTest {

    @Autowired
    private KafkaConsumer consumer;

    @Autowired
    private KafkaProducer producer;

    @Value("${test.topic}")
    private String topic;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() 
      throws Exception {
        producer.send(topic, "Sending with own simple KafkaProducer");
        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        
        assertThat(consumer.getLatch().getCount(), equalTo(0L));
        assertThat(consumer.getPayload(), containsString("embedded-test-topic"));
        log.info("Payload is {}", consumer.getPayload());
    }
}