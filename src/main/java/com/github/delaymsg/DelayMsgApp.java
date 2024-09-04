package com.github.delaymsg;

import com.github.delaymsg.constant.DelayConst;
import com.github.delaymsg.dao.DelayMsgDao;
import com.github.delaymsg.kafka.KafkaListener;
import com.github.delaymsg.kafka.KafkaSender;
import com.github.delaymsg.task.MsgStoreTask;
import com.github.delaymsg.task.MsgTransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * kafka 延时消息程序
 * 逻辑：
 * 1、业务系统先发延时消息到统一延时消息topic
 * 2、当前程序读取 topic 消息，保存于本地，提交偏移量
 * 3、扫描到达延时期限的消息，转发到实际业务topic
 * 4、删除本地延时消息
 *
 * @author yhh 2021-11-28 13:15:30
 **/
public class DelayMsgApp {

    private static final Logger log = LoggerFactory.getLogger(DelayMsgApp.class);

    public static void main(String[] args) {
        int workers = Integer.parseInt(DelayConst.WORKERS);
        int workSum = workers * 2;
        KafkaSender kafkaSender = KafkaSender.of(DelayConst.KAFKA_URL);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(workSum, workSum,
                0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        for (int i = 0; i < workers; i++) {
            DelayMsgDao delayDao = new DelayMsgDao(i);
            KafkaListener kafkaListener = KafkaListener.of(DelayConst.KAFKA_URL, DelayConst.KAFKA_GROUP_ID);
            threadPool.execute(new MsgStoreTask(delayDao, kafkaListener));
            threadPool.execute(new MsgTransferTask(delayDao, kafkaSender));
        }
        log.info("kafka 延时消息程序启动成功");
    }

}
