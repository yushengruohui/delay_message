package com.yhh.task;

import com.yhh.constant.DelayConst;
import com.yhh.dao.DelayDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.JsonUtils;
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
    private final DelayDao delayDao;
    private static final long SLEEP_TIME = 1000L;
    private static final KafkaSender KAFKA_PRODUCER = KafkaSender.of(DelayConst.KAFKA_URL);

    public MsgTransferTask(DelayDao delayDao) {
        this.delayDao = delayDao;
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
            int recordsSize = records.size();
            CountDownLatch countDownLatch = new CountDownLatch(recordsSize);
            List<Long> idList = new ArrayList<>(recordsSize);
            for (DelayDto record : records) {
                // 如果发送失败，重试最多100次，再失败则放弃这条消息
                Long id = record.getId();
                // 如果发送失败，重试最多16次，再失败则放弃这条消息
                KAFKA_PRODUCER.send(record.getTopic(), record.getMessageKey(), record.getMessage(),
                        (msg, exception) -> {
                            if (exception == null) {
                                idList.add(id);
                            } else {
                                //todo 发送mq失败，异常处理
                                log.warn("发送mq失败 || record : {}", JsonUtils.write(record));
                            }
                            countDownLatch.countDown();
                        }
                );
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                log.error("线程异常中断", e);
                Thread.currentThread().interrupt();
            }
            if (idList.isEmpty()) {
                continue;
            }
            delayDao.batchDelete(idList);
        }
        log.error("MsgTransferTask 异常中断，需人工排查");
    }

}