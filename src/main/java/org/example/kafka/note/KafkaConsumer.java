package org.example.kafka.note;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;

/**
 * @author xuejunze
 * @date 2021/9/17 11:55 上午
 **/
public class KafkaConsumer {
    public static void main(String[] args) {
        consume();
    }
    public static void consume() {

        // 获取连接
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = ClientConnection.createConsumer();
        // 2. 创建消费者实例并订阅topic
        String[] topics = new String[]{"demo-group"};
        consumer.subscribe(Arrays.asList(topics));

        // 3. 消费消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("消费者"+record);
            }
        }
    }
}
