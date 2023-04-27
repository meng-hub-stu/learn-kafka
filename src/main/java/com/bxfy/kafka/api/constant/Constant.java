package com.bxfy.kafka.api.constant;

/**
 * @Author Mengdexin
 * @date 2022 -05 -08 -21:35
 */
public interface Constant {

    /**
     * topic
     */
    String TOPIC_QUICKSTART = "topic-quickstart-test";
    /**
     * 消息回调
     */
    String TOPIC_NORMAL = "topic-normal";
    /**
     * 拦截器
     */
    String TOPIC_INTERCEPTOR = "topic-interceptor";
    /**
     * 序列化
      */
    String TOPIC_SERIALIZER = "topic-serializer";
    /**
     * 分区器
     */
    String TOPIC_PARTITION = "topic-partition";
    /**
     * 核心的方式
     */
    String TOPIC_CORE = "topic-core-01";

    /**
     * 平衡
     */
    String TOPIC_REBALANCE = "topic-rebalance";

    /**
     * 多线程
     */
    String TOPIC_THREAD = "topic-thread";

}
