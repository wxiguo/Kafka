package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Receiver {

    @KafkaListener(topics = "${app.topic.foo}")
    public void listen(ConsumerRecord record) {
        System.out.println(String.format("key: %s and value: %s", record.key(), record.value()));
    }
}
