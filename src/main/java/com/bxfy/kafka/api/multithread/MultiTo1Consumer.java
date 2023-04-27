package com.bxfy.kafka.api.multithread;

import com.bxfy.kafka.api.interceptor.CustomerConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bxfy.kafka.api.constant.Constant.TOPIC_INTERCEPTOR;
import static com.bxfy.kafka.api.constant.Constant.TOPIC_THREAD;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -20:24
 */
public class MultiTo1Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.221:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "thread-group");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        //自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        //消费端拦截器
//        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomerConsumerInterceptor.class.getName());
        int coreSize = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(coreSize);

        for (int i = 0; i < 5; i++) {
            executorService.submit(new MultiRunnable(properties, TOPIC_THREAD));
        }

    }


}
