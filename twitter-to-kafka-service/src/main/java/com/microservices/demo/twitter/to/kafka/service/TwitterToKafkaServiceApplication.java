package com.microservices.demo.twitter.to.kafka.service;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    public static Logger logger = LoggerFactory.getLogger(TwitterToKafkaServiceConfigData.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
    }

    public static void main(String... args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info(this.twitterToKafkaServiceConfigData.toString());
    }
}
