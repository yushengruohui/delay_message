package com.yhh.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
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
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(10000));
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
        boolean hasRecord = false;
        long idle = 1L;
        long busy = 100L;
        while (Thread.currentThread().isInterrupted()) {
            // 循环轮询，如果有数据则返回，最多循环 pollMs 毫秒
            ConsumerRecords<String, String> records = consumer.poll(hasRecord ? busy : idle);
            hasRecord = !records.isEmpty();
            if (hasRecord) {
                action.accept(records);
                // 如果数据都处理成功，则异步提交这次poll最后拉取的消息偏移量
                consumer.commitAsync(ignoreResponse);
            } else {
                SystemUtils.sleep(1000L);
            }
        }
    }

    /**
     * 以同步方式发送消息
     */
    public static boolean sendSync(KafkaProducer<String, String> producer, String topic, String msgKey, String msg, int maxRetry) {
        log.debug("topic : {} || msgKey : {} || msg : {} ", topic, msgKey, msg);
        for (int i = maxRetry - 1; i >= 0; i--) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgKey, msg);
            try {
                producer.send(record).get();
            } catch (Exception exception) {
                boolean canRetry = exception instanceof RetriableException || exception instanceof ExecutionException || exception instanceof InterruptedException;
                if (canRetry) {
                    log.warn("sendSync[kafka消息发送失败，准备重发] || 剩余次数 : {} || exception : {} || topic : {} || msgKey : {} || msg : {} ", i, exception, topic, msgKey, msg);
                    continue;
                }
                log.warn("sendSync[kafka消息发送失败，不可重试] || exception : {} || topic : {} || msgKey : {} || msg : {} ", exception, topic, msgKey, msg);
                return false;
            }
            return true;
        }
        return false;
    }

}
