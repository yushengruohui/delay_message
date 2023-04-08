package com.yhh.task;

import com.yhh.constant.DelayConst;
import com.yhh.dao.DelayDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.IdUtils;
import com.yhh.utils.JsonUtils;
import com.yhh.utils.kafka.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 抽取、保存延时消息线程
 *
 * @author yhh 2021-12-19 22:31:07
 **/
public class MsgStoreTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MsgStoreTask.class);
    private final DelayDao delayDao;

    public MsgStoreTask(DelayDao delayDao) {
        this.delayDao = delayDao;
    }

    @Override
    public void run() {
        log.info("读取 kafka topic[{}] 的消息，保存延时消息到本地[{}]", DelayConst.DELAY_TOPIC, DelayConst.STORE_PATH);
        KafkaListener consumer = KafkaListener.of(DelayConst.KAFKA_URL, DelayConst.KAFKA_GROUP_ID);
        consumer.subscribe(DelayConst.DELAY_TOPIC, records -> {
            int count = records.count();
            List<String> keys = new ArrayList<>(count);
            List<String> values = new ArrayList<>(count);
            for (ConsumerRecord<String, String> record : records) {
                String msg = record.value();
                log.debug("收到延时消息 : {}", msg);
                DelayDto delayDto = toDelayDto(msg);
                if (delayDto == null) {
                    continue;
                }
                keys.add(delayDto.getId());
                values.add(JsonUtils.write(delayDto));
            }
            if (values.isEmpty()) {
                return;
            }
            // 批量插入
            delayDao.batchStore(keys, values);
        });
        log.error("MsgStoreTask 被异常中断，需人工排查问题");
    }

    private DelayDto toDelayDto(String msg) {
        if (msg == null || msg.isEmpty() || msg.charAt(0) != '{') {
            log.warn("抛弃异常kafka消息 msg : {}", msg);
            return null;
        }
        DelayDto delayDto;
        try {
            delayDto = JsonUtils.read(msg, DelayDto.class);
        } catch (Exception e) {
            log.warn("json 反序列化失败，抛弃异常kafka消息: {}", msg, e);
            return null;
        }
        Long delayTime = delayDto.getDelayTime();
        if (delayTime == null || delayTime <= 0L || delayTime > System.currentTimeMillis()) {
            log.warn("delayTime[{}] 异常, 抛弃异常kafka消息: {}", delayTime, msg);
            return null;
        }
        String topic = delayDto.getTopic();
        if (topic == null || topic.isEmpty()) {
            // todo 排除不存在的topic
            log.warn("topic 不能为空, 抛弃异常kafka消息: {}", msg);
            return null;
        }
        String message = delayDto.getMessage();
        if (message == null || message.isEmpty()) {
            log.warn("message 不能为空, 抛弃异常kafka消息: {}", msg);
            return null;
        }
        // 保存到本地
        delayDto.setId(delayTime.toString() + IdUtils.nextId());
        return delayDto;
    }

}
