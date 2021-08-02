package com.hlink.data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaGenerator {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Properties props = new Properties();
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("bootstrap.servers", "172.18.1.21:9092");
        props.put("zookeeper.connect", "172.18.1.21:2181");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        Thread thread1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    JsonCurrencySender.sendMessage(props, 10);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        thread1.start();

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    JsonOrderSender.sendMessage(props, 30);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread2.start();

    }
}
