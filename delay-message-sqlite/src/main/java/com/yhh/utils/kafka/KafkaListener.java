package com.yhh.utils.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * kafka消费者助手，不是线程安全的对象，不要在多线程环境使用。
 * 使用案例：
 * <pre>
 *         KafkaListener listener = KafkaListener.of("127.0.0.1:9092", "sit_tester");
 *         listener.subscribe("test_topic", records -> {
 *             for (ConsumerRecord<String, String> record : records) {
 *                 String topic = record.topic();
 *                 int partition = record.partition();
 *                 String msg = record.value();
 *                 //业务处理
 *             }
 *         });
 * </pre>
 *
 * @author yhh 2022-05-07 19:58:15
 **/
public class KafkaListener {
    private static final OffsetCommitCallback IGNORE_RESPONSE = (k, v) -> {
    };
    /**
     * 长轮询，最大连接时限
     */
    private static final long LONG_POLL_MS = 5000L;
    private final KafkaConsumer<String, String> kafkaConsumer;

    private KafkaListener(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 自动提交偏移量，不推荐启用。如果业务处理异常，会丢失一批数据。
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // ENABLE_AUTO_COMMIT_CONFIG 为 true 时，下面这个配置才生效
        // properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        // 从最久消息开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 测试是否有数据时可能会采用从最新消息开始消费
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // poll()返回的最大记录条数[默认500]
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        // 当kafka版本低于0.10.1时，要求 ： 批量记录处理时间 < heartbeat.interval.ms < session.timeout.ms < request.timeout.ms
        // properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        // properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "305000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaConsumer::close));
    }

    /**
     * 创建kafka消息监听器（kafka消费者助手)
     *
     * @param bootstrapServers kafka节点，eg： ip:port,ip:port……
     * @param groupId          消费者组id
     * @return kafka消费者助手
     */
    public static KafkaListener of(String bootstrapServers, String groupId) {
        return new KafkaListener(bootstrapServers, groupId);
    }

    /**
     * 常规订阅消费。业务处理完后，才异步提交偏移量。
     * 业务处理注意保持幂等性，会有重复消费的可能。
     *
     * @param topicName     话题【 * 可模糊匹配】
     * @param recordHandler 消息处理器
     */
    public void subscribe(String topicName, Consumer<ConsumerRecords<String, String>> recordHandler) {
        subscribe(Collections.singleton(topicName), recordHandler);
    }

    /**
     * 常规订阅消费。业务处理完后，才异步提交偏移量。
     * 业务处理注意保持幂等性，会有重复消费的可能。
     *
     * @param topicNames    消息队列【 * 可模糊匹配】
     * @param recordHandler 消息处理器
     */
    public void subscribe(Collection<String> topicNames, Consumer<ConsumerRecords<String, String>> recordHandler) {
        kafkaConsumer.subscribe(topicNames);
        ConsumerRecords<String, String> records;
        Thread currentThread = Thread.currentThread();
        while (!currentThread.isInterrupted()) {
            records = kafkaConsumer.poll(LONG_POLL_MS);
            if (!records.isEmpty()) {
                // 对一批数据进行业务处理
                recordHandler.accept(records);
                // 如果数据都成功，则异步提交这次poll最后拉取的消息偏移量
                kafkaConsumer.commitAsync(IGNORE_RESPONSE);
            }
        }
    }

}