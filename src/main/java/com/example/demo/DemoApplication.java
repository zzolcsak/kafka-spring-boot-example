package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

    private static final String MY_TOPIC = "myTopic";
    public static final Logger logger = LoggerFactory.getLogger(DemoApplication.class);
    private final CountDownLatch latch = new CountDownLatch(3);

    @Autowired
    private KafkaTemplate<Integer, String> template;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args).close();
    }

    @Override
    public void run(String... args) throws Exception {
        this.template.send(MY_TOPIC, "foo1");
        this.template.send(MY_TOPIC, "foo2");
        this.template.send(MY_TOPIC, "foo3");
        latch.await(60, TimeUnit.SECONDS);
        logger.info("All received");
    }

    @KafkaListener(topics = "myTopic")
    public void listen(ConsumerRecord<?, ?> cr) {
        logger.info(cr.toString());
        latch.countDown();
    }

}
