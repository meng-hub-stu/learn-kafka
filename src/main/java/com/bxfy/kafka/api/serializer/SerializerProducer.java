package com.bxfy.kafka.api.serializer;

import com.bxfy.kafka.api.entity.User;
import com.bxfy.kafka.api.interceptor.CustomerProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.bxfy.kafka.api.constant.Constant.TOPIC_SERIALIZER;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -20:24
 */
public class SerializerProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.221:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "serializer-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //字符串的序列化
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //对象的序列化
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        //配置拦截器
//        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomerProducerInterceptor.class.getName());
        //进行简单测试
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(properties);
        for (int i = 0; i < 10; i++) {
            User user = new User("001", "张三");
            ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC_SERIALIZER, user);
            producer.send(record);
        }

        producer.close();
    }

}
