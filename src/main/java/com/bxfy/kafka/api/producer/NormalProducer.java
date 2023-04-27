package com.bxfy.kafka.api.producer;

import com.alibaba.fastjson.JSON;
import com.bxfy.kafka.api.entity.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bxfy.kafka.api.constant.Constant.TOPIC_NORMAL;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -20:24
 */
public class NormalProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.221:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "normal-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //重试机制
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);




        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        User user = new User("001", "xiao xiao");

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NORMAL, JSON.toJSONString(user));
        //同步发送消息
      /*  Future<RecordMetadata> metadataFuture = producer.send(record);
        RecordMetadata recordMetadata = metadataFuture.get();
        System.out.println(String.format("----打印数据信息 offset : %s, partition : %s, timestamp : %s",
                recordMetadata.offset(),
                recordMetadata.partition(),
                recordMetadata.timestamp()));*/
        //异步的处理
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (null != exception) {
                    exception.printStackTrace();
                }
                System.out.println(String.format("----打印数据信息 offset : %s, partition : %s, timestamp : %s",
                        recordMetadata.offset(),
                        recordMetadata.partition(),
                        recordMetadata.timestamp()));
            }
        });
        System.out.println("执行结束");
        producer.close();
    }

}
