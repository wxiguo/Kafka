package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerController extends Thread {

    private final KafkaConsumer consumer;

    public ConsumerController(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(100));
            for (ConsumerRecord record : records) {
                System.out.println(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerController("my-topic").start();

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "test");
//        props.setProperty("enable.auto.commit", "true");
//        props.setProperty("auto.commit.interval.ms", "1000");
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("my-topic", "foo", "bar"));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(100));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
//            }
//        }

//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "test");
//        props.setProperty("enable.auto.commit", "false");
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("my-topic", "foo", "bar"));
//        final int minBatchSize = 200;
//        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            for (ConsumerRecord<String, String> record : records) {
//                buffer.add(record);
//            }
//            if (buffer.size() >= minBatchSize) {
//                // 业务处理
////                insertIntoDb(buffer);
//                System.out.println(buffer.toString());
//                consumer.commitSync();
//                buffer.clear();
//            }
//        }

//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("group.id", "test");
//        props.setProperty("enable.auto.commit", "false");
//        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList("my-topic", "foo", "bar"));
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
//                for (TopicPartition partition : records.partitions()) {
//                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                    for (ConsumerRecord<String, String> record : partitionRecords) {
//                        System.out.println(record.offset() + ": " + record.value());
//                    }
//                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//                }
//            }
//        } finally {
//            consumer.close();
//        }
    }
}
