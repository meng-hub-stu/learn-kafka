package com.bxfy.kafka.api.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author Mengdexin
 * @date 2022 -05 -12 -17:23
 */
public class MultiRunnable implements Runnable{

    private Properties properties;

    private String topic;

    private KafkaConsumer<String, String> consumer;

    private static AtomicInteger atomic = new AtomicInteger(0);

    private static boolean isRunning = true;

    private String consumerName;

    public MultiRunnable(Properties properties, String topic){
        this.properties = properties;
        this.topic = topic;
        this.consumerName = "ThreadRunnable-" + atomic.getAndIncrement();
        System.out.println(String.format("当前的消费者start： %s", consumerName));
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                consumer = new KafkaConsumer<String, String>(properties);
                consumer.subscribe(Collections.singletonList(topic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                for (TopicPartition partition : records.partitions()){
                    List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
                    int size = consumerRecords.size();
                    System.out.println(String.format("消息总数：%s, partition: %s, topic: %s",
                            size,
                            partition.partition(),
                            topic));
                    for (int i = 0; i < consumerRecords.size(); i++ ){
                        ConsumerRecord<String, String> message = consumerRecords.get(i);
                        long offset = message.offset();
                        long commitOffset = offset + 1;
                        String value = message.value();
                        String key = message.key();
                        System.out.println(String.format("----当前消费者名称 ： %s,获取消息的key: %s, value: %s, 消息的offset：%s, 提交offset : %s, 当前消费者的香橙名称： %s",
                                consumerName,
                                key,
                                value,
                                offset,
                                commitOffset,
                                Thread.currentThread().getName()));
                    }

                }
            }

        } catch (Exception e){
            e.printStackTrace();
        }finally {
            if (null != consumer) {
                consumer.close();
            }
        }
    }

    public static boolean isIsRunning() {
        return isRunning;
    }

    public static void setIsRunning(boolean isRunning) {
        MultiRunnable.isRunning = isRunning;
    }

}
