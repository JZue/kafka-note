package org.example.kafka.note;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * @author xuejunze
 * @date 2021/9/17 11:54 上午
 **/
public class KafkaProducer {

    public static void main(String[] args) {
        produce();
    }

    public static void produce(){

// 2. 创建生产者实例
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = ClientConnection.createProducer();

// 3. 发送消息

// 0 异步发送消息
        for (int i = 0; i < 10; i++) {
            String data = "async :" + i;
            // 发送消息
            producer.send(new ProducerRecord<>("demo-group", data));
        }

// 1 同步发送消息 调用get()阻塞返回结果
        for (int i = 0; i < 10; i++) {
            String data = "sync : " + i;
            try {
                // 发送消息
                Future<RecordMetadata> send = producer.send(new ProducerRecord<>("demo-group", data));
                RecordMetadata recordMetadata = send.get();
                System.out.println("》》》》"+recordMetadata);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


// 2 异步发送消息 回调callback()
        for (int i = 0; i < 10; i++) {
            String data = "callback : " + i;
            // 发送消息
            producer.send(new ProducerRecord<>("demo-group", data), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 发送消息的回调
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("=="+metadata);
                    }
                }
            });
        }
        producer.close();

    }
}
