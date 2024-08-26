package com.yhh.task;

import com.yhh.dao.DelayMsgDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.SystemUtils;
import com.yhh.utils.kafka.KafkaSender;
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
            List<DelayDto> records = delayDao.scanTodoMsg();
            if (records.isEmpty()) {
                log.debug("暂无待转发的延时消息，休眠一下");
                SystemUtils.sleep(SLEEP_TIME);
                continue;
            }

            int count = records.size();
            ArrayList<String> toDeleteIds = new ArrayList<>(count);
            transfer(records, new CountDownLatch(count), toDeleteIds);

            if (!toDeleteIds.isEmpty()) {
                delayDao.batchDelete(toDeleteIds);
            }
        }
        log.error("MsgTransferTask 异常中断，需人工排查");
    }

    public void transfer(List<DelayDto> records, CountDownLatch countDownLatch, List<String> toDeleteIds) {
        for (DelayDto record : records) {
            String id = record.getId();
            String topic = record.getTopic();
            String key = record.getMessageKey();
            String message = record.getMessage();
            kafkaSender.send(topic, key, message, (msg, exception) -> {
                if (exception == null) {
                    toDeleteIds.add(id);
                } else {
                    //todo 发送mq失败，异常处理
                    log.warn("发送mq失败 || id : {}", id);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("线程异常中断", e);
            Thread.currentThread().interrupt();
        }
    }

}