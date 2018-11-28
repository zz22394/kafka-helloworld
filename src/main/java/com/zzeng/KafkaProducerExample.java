package com.zzeng;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerExample {
    private final static String TOPIC_NAME = "topic_name";

    public static void main(String[] args) {
        // produce a test message
        // if u run this multiple times ... u will have multiple messages in the
        // test_topic topic (as would be expected)
        Producer<String, String> producer = KafkaProducerExample.createProducer();

        Scanner sc = new Scanner(System.in);
        try {
            while (true) {
                System.out.print("> ");
                String text = sc.nextLine();

                ProducerRecord<String, String> recordToSend = new ProducerRecord<String, String>(TOPIC_NAME, "message",
                        text + " , timeInMillis=" + System.currentTimeMillis());
                try {
                    // synchronous send.... get() waits for the computation to
                    // finish
                    RecordMetadata rmd = producer.send(recordToSend).get();
                    System.out.printf("Message Sent ==>> topic = %s, partition = %s, offset = %d\n", rmd.topic(),
                            rmd.partition(), rmd.offset());
                } catch (Exception ex) {
                    // this is test code...so don't judge me !!
                    ex.printStackTrace();
                }

                if (text.equalsIgnoreCase("exit")) {
                    break;
                }
            }
        } finally {
            sc.close();
        }
    }

    private static Producer<String, String> createProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "zzeng-hdp-1.field.hortonworks.com:6667,zzeng-hdp-2.field.hortonworks.com:6667,zzeng-hdp-3.field.hortonworks.com:6667");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(kafkaProps);
    }

}
