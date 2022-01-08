package com.yhh.task;

import com.yhh.constant.DelayConst;
import com.yhh.dao.DelayDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
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
    /**
     * 重试次数上限
     */
    private static final int MAX_RETRY = 16;
    private final DelayDao delayDao;

    public MsgTransferTask(DelayDao delayDao) {
        this.delayDao = delayDao;
    }

    @Override
    public void run() {
        // 每个线程一个kafka生产者[快速转发延时消息，防止阻塞]
        KafkaProducer<String, String> kafkaProducer = KafkaUtils.createProducer(DelayConst.KAFKA_URL, 1, Runtime.getRuntime().availableProcessors() * 2);
        int sleepTime = 1;
        List<DelayDto> records = new ArrayList<>();
        //noinspection InfiniteLoopStatement
        while (true) {
            delayDao.scanTodoMsg(records);
            if (records.isEmpty()) {
                log.debug("暂无待转发的延时消息，休眠一下");
                try {
                    // 睡眠 sleepTime * 512 ms
                    //noinspection BusyWait
                    Thread.sleep(sleepTime << 9);
                } catch (InterruptedException ignored) {
                }
                if (sleepTime < 8) {
                    // sleepTime = sleepTime * 2;
                    sleepTime <<= 1;
                }
                continue;
            }
            if (sleepTime > 1) {
                sleepTime = 1;
            }
            int recordsSize = records.size();
            List<Long> idList = new ArrayList<>(recordsSize);
            CountDownLatch countDownLatch = new CountDownLatch(recordsSize);
            for (DelayDto record : records) {
                // 如果发送失败，重试最多16次，再失败则放弃这条消息
                KafkaUtils.send(kafkaProducer,
                        record.getTopic(),
                        record.getMessageKey(),
                        record.getMessage(),
                        MAX_RETRY,
                        (recordMetadata, exception) -> countDownLatch.countDown());
                idList.add(record.getId());
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException ignored) {
            }
            delayDao.batchDelete(idList);
            records.clear();
        }
    }

}