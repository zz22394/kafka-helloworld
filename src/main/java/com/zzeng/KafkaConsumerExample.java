package com.zzeng;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerExample {
//    private final static String TOPIC_NAME = "topic_name";
    private final static String TOPIC_NAME = "Movies";

    public static void main(String[] args) {
        // consume messages
        Consumer<String, String> consumer = KafkaConsumerExample.createConsumer();

        // subscribe to the test topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        try {
            // loop forever (hmmmmm)
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                // print the messages received
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Message received ==> topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

            }
        } finally {
            consumer.close();
        }
    }

    private static Consumer<String, String> createConsumer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "zzeng-hdp-1.field.hortonworks.com:6667,zzeng-hdp-2.field.hortonworks.com:6667,zzeng-hdp-3.field.hortonworks.com:6667");
        kafkaProps.put("group.id", "test_consumer_group");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(kafkaProps);
    }
}
