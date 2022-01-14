package com.microservices.demo.common.config;

import com.microservices.demo.config.RetryConfigData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    private final RetryConfigData retryConfigData;

    public RetryConfig(RetryConfigData retryConfigData) {
        this.retryConfigData = retryConfigData;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        var retryTemplate = new RetryTemplate();
        var exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(retryConfigData.getInitialIntervalMs());
        exponentialBackOffPolicy.setMultiplier(retryConfigData.getMultiplier());
        exponentialBackOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);
        var simpleRetryPolicty = new SimpleRetryPolicy();
        simpleRetryPolicty.setMaxAttempts(retryConfigData.getMaxAttempts());
        retryTemplate.setRetryPolicy(simpleRetryPolicty);
        return retryTemplate;
    }

}