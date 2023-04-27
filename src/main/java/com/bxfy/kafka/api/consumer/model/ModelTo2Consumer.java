package com.bxfy.kafka.api.consumer.model;

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

import static com.bxfy.kafka.api.constant.Constant.TOPIC_CORE;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -20:24
 */
public class ModelTo2Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.221:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "core-group");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        //自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        //消费端拦截器
//        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomerConsumerInterceptor.class.getName());

        KafkaConsumer<String, String > consumer = new KafkaConsumer<String, String>(properties);
        //指定的topic进行订阅消息，可以是数组
        consumer.subscribe(Collections.singletonList(TOPIC_CORE));
        System.out.println("---partitionstart consumer started ------");
        //进行获取消息内容
        try{
            while (true) {
                //拉取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                for(TopicPartition partition : records.partitions()){
                    //获得分区
                    List<ConsumerRecord<String, String>> record = records.records(partition);
                    //topic对应的分区
                    String topic = partition.topic();
                    int size = record.size();
                    System.out.println(String.format("----获取topic : %s, 分区位置 : %s, 消息总数 ： %s----",
                            topic,
                            partition.partition(),
                            size));
                    for (int i = 0; i< size; i++) {
                        //实际收到的数据
                        ConsumerRecord<String, String> result = record.get(i);
                        String key = result.key();
                        String value = result.value();
                        long offset = result.offset();
                        long commitOffset = offset + 1;
                        System.out.println(String.format("----获取消息的key: %s, value: %s, 消息的offset：%s, 提交offset : %s",
                                key,
                                value,
                                offset,
                                commitOffset));
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }


}
