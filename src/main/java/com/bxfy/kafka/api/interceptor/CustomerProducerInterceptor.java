package com.bxfy.kafka.api.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author Mengdexin
 * @date 2022 -05 -09 -21:17
 */
public class CustomerProducerInterceptor implements ProducerInterceptor<String, String> {

    private volatile int success = 0;
    private volatile int failure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.err.println("----发送消息之前----");
        String modifyValue = "prefix" + record.value();
        return new ProducerRecord<String, String >(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                modifyValue,
                record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.err.println("----发送消息之后-----");
        if (null == exception) {
            success ++;
        } else {
            failure ++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) success / success + failure;
        System.out.println(String.format("生产者关闭，发送消息的成功率为: %s %%", successRatio * 100));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

}
