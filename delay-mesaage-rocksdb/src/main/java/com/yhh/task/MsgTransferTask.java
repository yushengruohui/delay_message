package com.yhh.task;

import com.yhh.constant.DelayConst;
import com.yhh.dao.DelayDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.KafkaUtils;
import com.yhh.utils.SystemUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
    private static final long SLEEP_TIME = 1000L;
    private static final KafkaProducer<String, String> KAFKA_PRODUCER = KafkaUtils.createProducer(DelayConst.KAFKA_URL, 1, Runtime.getRuntime().availableProcessors() * 2);
    private final DelayDao delayDao;

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
            List<String> idList = new ArrayList<>(recordsSize);
            for (DelayDto record : records) {
                // 如果发送失败，重试最多16次，再失败则放弃这条消息
                boolean isOk = KafkaUtils.sendSync(KAFKA_PRODUCER,
                        record.getTopic(),
                        record.getMessageKey(),
                        record.getMessage(),
                        MAX_RETRY
                );
                if (isOk) {
                    idList.add(record.getId());
                }
                // todo 后续考虑处理发送失败。
            }
            if (idList.isEmpty()) {
                continue;
            }
            delayDao.batchDelete(idList);
        }
        log.error("MsgTransferTask 异常中断，需人工排查");
    }

}