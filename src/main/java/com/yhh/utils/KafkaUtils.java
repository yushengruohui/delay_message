package com.yhh.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * kafka工具类
 *
 * @author yhh 2021-11-02 22:34:16
 **/
public class KafkaUtils {
    private KafkaUtils() {
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);


    /**
     * 创建常用的 kafka 生产者[多线程安全]，建议配置为单例模式。
     *
     * @param bootstrapServers kafka连接地址[ip:port,ip:port……]
     * @param ackMode          ack模式[-1: 高可靠，低吞吐量 ||0: 高吞吐量，不可靠 || 1: 均衡]
     * @param flight           单连接并发请求数[顺序消息发送 : 1|| 高吞吐量 : cpu核心*2 ]
     * @return
     */
    public static KafkaProducer<String, String> createProducer(String bootstrapServers, int ackMode, int flight) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(ackMode));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(flight));
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(80 << 10));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(125 << 20));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(TimeUnit.MINUTES.toMillis(3)));
        return new KafkaProducer<>(properties);
    }


    /**
     * 创建常用的 kafka 消费者。
     * 消费者是线程不安全的，禁止多线程复用同一消费者
     * 建议程序关闭时，关闭连接：
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaConsumer.close(Duration.ofSeconds(3))));
     *
     * @param bootstrapServers kafka连接地址[ip:port,ip:port……]
     * @param groupId          消费者组id
     * @return
     */
    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(1 << 20));
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(50 << 20));
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(500));
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(10000));
        // properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        // properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        // properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        // properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(TimeUnit.MINUTES.toMillis(2)));
        return new KafkaConsumer<>(properties);
    }


    /**
     * 常规订阅消费。业务处理注意保持幂等性，会有重复消费的可能
     * 如果没有数据则睡眠一定时间，减低cpu使用率
     *
     * @param consumer 消费者
     * @param topic    消息队列【 * 可模糊匹配】
     * @param action   业务处理
     */
    public static void subscribe(KafkaConsumer<String, String> consumer, String topic, Consumer<? super ConsumerRecords<String, String>> action) {
        consumer.subscribe(Collections.singleton(topic));
        OffsetCommitCallback ignoreResponse = (k, v) -> {
        };
        int sleep = 1;
        boolean hasRecord = false;
        long idle = 1L;
        long busy = 100L;
        //noinspection InfiniteLoopStatement
        while (true) {
            // 循环轮询，如果有数据则返回，最多循环 pollMs 毫秒
            ConsumerRecords<String, String> records = consumer.poll(hasRecord ? busy : idle);
            hasRecord = records != null && !records.isEmpty();
            if (hasRecord) {
                action.accept(records);
                // 如果数据都处理成功，则异步提交这次poll最后拉取的消息偏移量
                consumer.commitAsync(ignoreResponse);
                if (sleep > 1) {
                    sleep = 1;
                }
            } else {
                try {
                    //noinspection BusyWait
                    Thread.sleep(sleep << 9);
                } catch (InterruptedException ignored) {
                }
                if (sleep < 8) {
                    // sleep=sleep*2 , sleep 最大值为 8
                    sleep <<= 1;
                }
            }
        }
    }

    /**
     * kafka 常用发消息送方法
     * 在 ack=1 情况下，尽可能保证消息成功发送。【异常时消息乱序发送】
     * kafka 自带重试机制无法解决的常见异常：
     * 1、超过 request.timeout.ms 未响应的消息[kafka节点异常]
     * 2、获取 metadata 失败[kafka节点异常]
     * 3、kafka 缓存池满了，超过阻塞时限[生产速度大于broker写入速度]
     *
     * @param producer 生产者
     * @param topic    消息队列
     * @param msgKey   消息关键字
     * @param msg      消息
     * @param maxRetry 最大重试次数
     * @param callback 消息发送回调处理事件
     */
    public static void send(KafkaProducer<String, String> producer, String topic, String msgKey, String msg, int maxRetry, Callback callback) {
        trySend(producer, topic, msgKey, msg, new AtomicInteger(maxRetry), callback);
    }

    private static void trySend(KafkaProducer<String, String> producer, String topic, String msgKey, String msg, AtomicInteger remainRetry, Callback callback) {
        // 一定要新建一个 ProducerRecord ，发送超时后，ProducerRecord会被设置为 null
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, msg);
        producer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                if (callback != null) {
                    callback.onCompletion(recordMetadata, null);
                }
            } else if (exception instanceof RetriableException) {
                if (remainRetry.getAndDecrement() == 0) {
                    log.error("trySend[重发次数达到上限，kafka消息发送失败] || exception : {} || topic : {} || msgKey : {} || msg : {} ", exception, topic, msgKey, msg);
                    if (callback != null) {
                        callback.onCompletion(recordMetadata, exception);
                    }
                    return;
                }
                log.warn("trySend[kafka消息发送失败，准备重发] || remainRetry : {} || exception : {} || topic : {} || msgKey : {} || msg : {} ", remainRetry.get(), exception, topic, msgKey, msg);
                trySend(producer, topic, msgKey, msg, remainRetry, callback);
            } else {
                if (remainRetry.getAndSet(0) == 0) {
                    log.error("trySend[未知异常，kafka消息发送失败] ||  topic : {} || msgKey : {} || msg : {} ", topic, msgKey, msg, exception);
                    if (callback != null) {
                        callback.onCompletion(recordMetadata, exception);
                    }
                    return;
                }
                // 未知异常，重试一次
                trySend(producer, topic, msgKey, msg, remainRetry, callback);
            }
        });
    }

}
