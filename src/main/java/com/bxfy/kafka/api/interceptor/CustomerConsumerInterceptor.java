package com.bxfy.kafka.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @Author Mengdexin
 * @date 2022 -05 -09 -21:43
 */
public class CustomerConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /**
     * 消费端收到消息处理之前
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("-----消费端消费消息之前---------");
        return records;
    }

    /**
     * 消费端处理完消息之后
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("-----消费端消费消息之后-------");
        offsets.forEach((tp, offset) -> {
            System.out.println(String.format("消费者处理完成， 分区：%s , 偏移量 ： %s",
                    tp,
                    offset));
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

}
