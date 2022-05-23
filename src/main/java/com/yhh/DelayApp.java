package com.yhh;

import com.yhh.constant.DelayConst;
import com.yhh.dao.DelayDao;
import com.yhh.task.MsgStoreTask;
import com.yhh.task.MsgTransferTask;
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
public class DelayApp {
    private static final Logger log = LoggerFactory.getLogger(DelayApp.class);

    public static void main(String[] args) {
        int workers = Integer.parseInt(DelayConst.WORKERS);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(workers * 2, workers * 2, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        for (int i = 0; i < workers; i++) {
            DelayDao delayDao = new DelayDao(i);
            threadPool.execute(new MsgStoreTask(delayDao));
            threadPool.execute(new MsgTransferTask(delayDao));
        }
        log.info("kafka 延时消息程序启动成功");
    }
}
