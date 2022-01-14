package com.microservices.demo.twitter.to.kafka.service.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import javax.annotation.PreDestroy;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static Logger logger = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        var cb = new ConfigurationBuilder();
        cb.setDebugEnabled(this.twitterToKafkaServiceConfigData.isTwitterStreamDebugEnabled())
            .setOAuthConsumerKey(this.twitterToKafkaServiceConfigData.getTwitterStreamApiKey())
            .setOAuthConsumerSecret(this.twitterToKafkaServiceConfigData.getTwitterStreamApiKeySecret())
            .setOAuthAccessToken(this.twitterToKafkaServiceConfigData.getTwitterAccessToken())
            .setOAuthAccessTokenSecret(this.twitterToKafkaServiceConfigData.getTwitterAccessTokenSecret());

        this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (this.twitterStream != null) {
            logger.info("Shutting down Twitter stream");
            this.twitterStream.shutdown();
            this.twitterStream = null;
        }
    }

    private void addFilter() {
        var twitterKeywords = this.twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        var filterQuery = new FilterQuery(twitterKeywords);
        this.twitterStream.filter(filterQuery);
    }
}
