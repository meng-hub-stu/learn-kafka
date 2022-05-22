package com.bxfy.kafka.api.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author Mengdexin
 * @date 2022 -05 -10 -17:55
 */
public class CustomerPartition implements Partitioner {

    private static final AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key,
                         byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        Integer numPartitions = cluster.partitionCountForTopic(topic);
        System.err.println(String.format("-------自定义partition, 分区器的数量：%s, keyBytes： %s", numPartitions, keyBytes));
        if (null == keyBytes) {
            return atomicInteger.getAndIncrement() % numPartitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
