package com.github.delaymsg.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * kafka消息发送助手。多线程安全，建议设计为单例模式。
 * 如果有特殊需求，也可以创建多个发送助手，并发发送，可以提高发送效率。
 * 使用案例：
 * <pre>
 * KafkaSender kafkaSender = KafkaSender.of("127.0.0.1:9092");
 * kafkaSender.send("test_topic", "user_id", "{}");
 * </pre>
 *
 * @author yhh 2022-05-07 12:46:41
 **/
public class KafkaSender {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaSender(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    private KafkaSender(String bootstrapServers) {
        int flight = Runtime.getRuntime().availableProcessors() * 2;
        Properties properties = createProducerProperties(bootstrapServers, flight);
        kafkaProducer = new KafkaProducer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close));
    }

    /**
     * 创建kafka消息发送助手
     *
     * @param bootstrapServers 服务器地址，eg : ip:port,ip:port……
     * @return
     */
    public static KafkaSender of(String bootstrapServers) {
        return new KafkaSender(bootstrapServers);
    }

    /**
     * 异步发送消息。如果遇到可重试异常，最多重试100次。
     *
     * @param topic    话题
     * @param msgKey   消息的key
     * @param msg      消息体
     * @param callback 回调处理对象
     */
    public void send(String topic, String msgKey, String msg, Callback callback) {
        log.debug("topic: {} || msgKey : {}|| msg : {}", topic, msgKey, msg);
        trySend(topic, msgKey, msg, callback, new AtomicInteger(100));
    }

    private static Properties createProducerProperties(String bootstrapServers, int flight) {
        Properties properties = new Properties();
        // 多个以 , 分割
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // 消息确定方式。0：不用broker响应 || 1: 一个broker响应即可 || all： 发送至topic的所有broker都响应。
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        // 失败重试次数。
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // 推送最大并发数，默认值[5]，如果要确保消息推送顺序则为1，否则为cpu核数*2
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, flight);
        // 批量发送缓存大小，单位：字节。
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 80 << 10);
        // 发送间隙少于多少毫秒，则合并入一个批次
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 消息缓存区大小，单位：字节。默认值 32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128 << 20);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return properties;
    }

    /**
     * 尽量发送消息，如果是可重试异常，可以最多重试retry次。
     * 主要解决 kafka 自带重试机制无法解决的异常：
     * 1、超过 request.timeout.ms 未发送的消息[kafka节点异常]
     * 2、获取 metadata 失败[kafka节点异常]
     *
     * @param retry 重试次数
     */
    private void trySend(String topic, String msgKey, String msg, Callback callback, AtomicInteger retry) {
        // 一定要新建一个 ProducerRecord ，发送超时后，ProducerRecord会被设置为 null
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, msg);
        kafkaProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                // 成功发送消息，回调处理
                if (callback != null) {
                    callback.onCompletion(recordMetadata, null);
                }
                return;
            }
            // 不可重试异常处理
            if (!(exception instanceof RetriableException)) {
                log.error("kafka producer 发送消息失败[不可恢复的异常]。 exception : {} || topic : {} || msgKey : {} || msg : {} ", exception, topic, msgKey, msg);
                if (callback != null) {
                    callback.onCompletion(recordMetadata, exception);
                }
                return;
            }
            // 可重试异常处理
            if (retry.getAndDecrement() == 0) {
                log.error("kafka producer 发送消息失败[达到重试次数上限，仍未恢复正常] || exception : {} || topic : {} || msgKey : {} || msg : {} ", exception, topic, msgKey, msg);
                if (callback != null) {
                    callback.onCompletion(recordMetadata, exception);
                }
                return;
            }
            log.warn("kafka producer 发送消息失败，准备重试 || 剩余重试次数 : {} || exception : {} || topic : {} || msgKey : {} || msg : {} ", retry.get(), exception, topic, msgKey, msg);
            trySend(topic, msgKey, msg, callback, retry);
        });
    }

}