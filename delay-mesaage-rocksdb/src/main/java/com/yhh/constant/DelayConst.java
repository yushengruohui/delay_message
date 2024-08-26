package com.yhh.constant;

import com.yhh.utils.PropertyUtils;

/**
 * @author yhh 2021-12-19 10:48:36
 **/
public final class DelayConst {

    private static final String CONFIG_FILE = "kafka.properties";

    /**
     * kafka 连接url [ip:port,ip:port……]
     */
    public static final String KAFKA_URL = PropertyUtils.getConfig(CONFIG_FILE, "kafka.url", "127.0.0.1:9003");

    /**
     * 消费者组
     */
    public static final String KAFKA_GROUP_ID = PropertyUtils.getConfig(CONFIG_FILE, "kafka.delay.group.id", "delay_msg_app");

    /**
     * 延时消息本地存储路径，建议使用绝对值。
     * 目录不存在时会自动创建目录
     */
    public static final String STORE_PATH = PropertyUtils.getConfig(CONFIG_FILE, "kafka.delay.store.path", "/var/delay_message/db");

    /**
     * 统一延时消息topic
     */
    public static final String DELAY_TOPIC = PropertyUtils.getConfig(CONFIG_FILE, "kafka.delay.topic", "common_delay_msg");

    /**
     * 并发处理数。
     * 每个处理流程都会创建一个kafka消费者，sqlite数据库。
     * 配置大一些可以提高处理速度，但注意不要超过topic的分区数，否则超出部分不会接受延时消息。
     * 需要自行测试合适的并发处理数，磁盘io、网络io达到上限后，再添加也是无效的。
     */
    public static final String WORKERS = PropertyUtils.getConfig(CONFIG_FILE, "kafka.delay.workers", "2");

    private DelayConst() {
    }

}
