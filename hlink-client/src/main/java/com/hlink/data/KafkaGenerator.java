package com.hlink.data;

import com.hyt.rtdw.config.KafkaConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaGenerator {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        Properties props = KafkaConfig.buildKafkaProps();
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

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

        Thread thread3 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    JsonOrderDetailSender.sendMessage(props, 10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread3.start();
    }
}
