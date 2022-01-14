package com.microservices.demo.kafka.admin.exception;

public class KafkaClientException extends RuntimeException {
    public KafkaClientException(String message, Throwable t) {
        super(message, t);
    }

    public KafkaClientException(String message) {
        super(message);
    }
}
