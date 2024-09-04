package com.github.delaymsg.task;

import com.github.delaymsg.dao.DelayMsgDao;
import com.github.delaymsg.dto.DelayDto;
import com.github.delaymsg.kafka.KafkaSender;
import com.github.delaymsg.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 延时消息转发线程
 *
 * @author yhh 2021-12-19 22:32:35
 **/
public class MsgTransferTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MsgTransferTask.class);

    private static final long SLEEP_TIME = 1000L;

    private final KafkaSender kafkaSender;

    private final DelayMsgDao delayDao;

    public MsgTransferTask(DelayMsgDao delayDao, KafkaSender kafkaSender) {
        this.delayDao = delayDao;
        this.kafkaSender = kafkaSender;
    }

    @Override
    public void run() {
        Thread currentThread = Thread.currentThread();
        while (!currentThread.isInterrupted()) {
            scanAndTransfer();
        }
        log.error("MsgTransferTask 异常中断，需人工排查");
    }

    public void scanAndTransfer() {
        List<DelayDto> messages = delayDao.scanTodoMsg();
        if (messages.isEmpty()) {
            log.debug("暂无待转发的延时消息，休眠一下");
            SystemUtils.sleep(SLEEP_TIME);
            return;
        }

        transferAndDeleted(messages);
    }

    private void transferAndDeleted(List<DelayDto> records) {
        int count = records.size();
        List<String> toDeleteIds = new ArrayList<>(count);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (DelayDto record : records) {
            sendMsg(record, countDownLatch, toDeleteIds);
        }

        awaitSend(countDownLatch);
        if (!toDeleteIds.isEmpty()) {
            delayDao.batchDelete(toDeleteIds);
        }
    }

    private void sendMsg(DelayDto record, CountDownLatch countDownLatch, List<String> toDeleteIds) {
        String id = record.getId();
        String topic = record.getTopic();
        String key = record.getMessageKey();
        String message = record.getMessage();
        kafkaSender.send(topic, key, message, (msg, exception) -> {
            if (exception == null) {
                toDeleteIds.add(id);
            } else {
                log.warn("发送mq失败 || id : {}", id);
            }
            countDownLatch.countDown();
        });
    }

    private void awaitSend(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("线程异常中断", e);
            Thread.currentThread().interrupt();
        }
    }

}