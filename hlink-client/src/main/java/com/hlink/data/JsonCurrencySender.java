package com.hlink.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class JsonCurrencySender {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final SendCallBack sendCallBack = new SendCallBack();
    private static final String topicName = "currency_table";
    private static final Map<String, Integer> currency2rates = initCurrency2rates();
    private static final Map<String, String> country2currency = initCountry2Currency();

    public static void sendMessage(Properties kafkaProperties, int continueMinutes) throws InterruptedException, JsonProcessingException {
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(kafkaProperties);
        //update currency per 30 seconds
        for (int i = 0; i < (continueMinutes * 60 / 30); i++) {
            long timestart = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : country2currency.entrySet()) {
                Map<String, Object> map = new HashMap<>();
                map.put("country", entry.getKey());
                map.put("currency", entry.getValue());
                map.put("rate", currency2rates.get(entry.getValue()) + 1);
                Long time = System.currentTimeMillis();
                DateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                Date date = new Date(time);
                String jsonSchemaDate = dateFormat.format(date);
                map.put("ts_time", time);
                map.put("currency_time", jsonSchemaDate);
                producer.send(
                        new ProducerRecord<>(
                                topicName,
                                String.valueOf(time),
                                objectMapper.writeValueAsString(map)
                        ), sendCallBack

                );
                System.out.println( objectMapper.writeValueAsString(map));
            }
            long timecast = System.currentTimeMillis() - timestart;
            System.out.println((i + 1) * currency2rates.size() + " has sent to topic:[" + topicName + "] in " + timecast + "ms");
            if (timecast < 30 * 1000) {
                Thread.sleep(30 * 1000 - timecast);
            }
        }
    }

    static class SendCallBack implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static Map<String, Integer> initCurrency2rates() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("US Dollar", 102);
        map.put("Euro", 114);
        map.put("Yen", 1);
        map.put("RMB", 16);
        return map;
    }

    private static Map<String, String> initCountry2Currency() {
        final Map<String, String> map = new HashMap<>();
        map.put("America", "US Dollar");
        map.put("German", "Euro");
        map.put("Japan", "Yen");
        map.put("China", "RMB");
        return map;
    }
}
