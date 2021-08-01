package com.hlink.data;

import com.dtstack.flinkx.client.util.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class MyKafkaProducer implements Runnable{


    private KafkaProducer<String, String> producer;
    private Map<String, String> data;
    private final String topic;

    public MyKafkaProducer(String topicName) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "172.18.1.21:9092");
        props.put("zookeeper.connect","172.18.1.21:2181");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topicName;
    }

    public void loadData(String filePath){
       this.data = JsonUtil.loadLineJson(filePath);
    }


    @Override
    public void run() {

        int messageNo = 1;
        Random rand = new Random();
        try {
            for(;;) {
                int i = rand.nextInt(data.size());
                Set<String> keys = data.keySet();
                ArrayList<String> lists = new ArrayList(keys);
                String messageStr = data.get(lists.get(i));
                ProducerRecord record = new ProducerRecord<String, String>(topic, null, String.valueOf(i), messageStr);
                producer.send(record);
                //生产了100条就打印
                if(messageNo%10==0){
                    System.out.println("发送的信息:" + messageStr);
                }
                //生产1000条就退出
                if(messageNo%1000==0){
                    System.out.println("成功发送了"+messageNo+"条");
                    break;
                }
                messageNo++;
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
         MyKafkaProducer test = new  MyKafkaProducer("da");
        test.loadData("E:\\code\\flinkx\\flinkx-examples\\json\\kafka\\kafka_stream_data.txt");
        test.run();
    }
}
