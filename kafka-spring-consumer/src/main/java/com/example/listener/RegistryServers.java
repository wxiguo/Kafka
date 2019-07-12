package com.example.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class RegistryServers implements MessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord record) {
        System.out.println("接收到消息：");
        System.out.println(record.value());
    }
}
