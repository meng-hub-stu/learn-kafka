package com.bxfy.kafka.api.quickstart;

import com.alibaba.fastjson.JSON;
import com.bxfy.kafka.api.entity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.bxfy.kafka.api.constant.Constant.TOPIC_QUICKSTART;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -20:24
 */
public class QuickStartProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.221:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "quickstart-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //进行简单测试
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            User user = new User("001", "张三");
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_QUICKSTART, JSON.toJSONString(user));
            System.out.println("发送消息");
            producer.send(record);
            System.out.println("发送成功");
        }

        producer.close();
    }

}
