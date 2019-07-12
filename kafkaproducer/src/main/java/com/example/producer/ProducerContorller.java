package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerContorller extends Thread {

    private final KafkaProducer<String, String> producer;

    private final String topic;

    public ProducerContorller(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

    public static void main(String[] args) {
        new ProducerContorller("my-topic").start();

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("acks", "all");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer producer = new KafkaProducer<>(props);
//        for (int i = 0; i < 100; i++) {
//            producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
//        }
//        producer.close();

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("transactional.id", "my-transactional-id");
//        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
//
//        producer.initTransactions();
//
//        try {
//            producer.beginTransaction();
//            for (int i = 0; i < 100; i++)
//                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
//            producer.commitTransaction();
//        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
//            // 异常，关闭生产者
//            producer.close();
//        } catch (KafkaException e) {
//            // 异常，重试
//            producer.abortTransaction();
//        }
//        producer.close();
    }
}
