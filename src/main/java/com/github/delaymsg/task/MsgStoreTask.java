package com.github.delaymsg.task;

import com.github.delaymsg.constant.DelayConst;
import com.github.delaymsg.dao.DelayMsgDao;
import com.github.delaymsg.dto.DelayDto;
import com.github.delaymsg.utils.FstUtils;
import com.github.delaymsg.utils.IdUtils;
import com.github.delaymsg.utils.JsonUtils;
import com.github.delaymsg.utils.kafka.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 抽取、保存延时消息线程
 *
 * @author yhh 2021-12-19 22:31:07
 **/
public class MsgStoreTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MsgStoreTask.class);

    private final DelayMsgDao delayDao;

    private final KafkaListener consumer;

    public MsgStoreTask(DelayMsgDao delayDao, KafkaListener kafkaListener) {
        this.delayDao = delayDao;
        consumer = kafkaListener;
    }

    @Override
    public void run() {
        log.info("读取 kafka topic[{}] 的消息，保存延时消息到本地[{}]", DelayConst.DELAY_TOPIC, DelayConst.STORE_PATH);
        consumer.subscribe(DelayConst.DELAY_TOPIC, records -> storeMsg(records));
        if (Thread.currentThread().isInterrupted()) {
            log.error("MsgStoreTask 被异常中断，需人工排查问题");
        }
    }

    public void storeMsg(ConsumerRecords<String, String> records) {
        int count = records.count();
        List<String> keys = new ArrayList<>(count);
        List<byte[]> values = new ArrayList<>(count);
        for (ConsumerRecord<String, String> record : records) {
            String msg = record.value();
            log.debug("收到延时消息 : {}", msg);
            toDelayDto(msg).ifPresent(dto -> {
                keys.add(dto.getId());
                values.add(FstUtils.write(dto));
            });
        }
        if (values.isEmpty()) {
            return;
        }
        // 批量插入
        delayDao.batchStore(keys, values);
    }

    private Optional<DelayDto> toDelayDto(String msg) {
        if (msg == null || msg.isEmpty() || msg.charAt(0) != '{') {
            log.warn("抛弃异常kafka消息 msg : {}", msg);
            return Optional.empty();
        }
        return JsonUtils.read(msg, DelayDto.class)
                .filter(dto -> dto.checkFormat())
                .map(dto -> {
                    dto.setId(dto.getTriggerTime().toString() + IdUtils.nextId());
                    return dto;
                });
    }

}
