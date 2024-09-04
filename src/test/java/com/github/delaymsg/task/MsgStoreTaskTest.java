package com.github.delaymsg.task;

import com.github.delaymsg.dao.DelayMsgDao;
import com.github.delaymsg.kafka.KafkaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author HuaHui Yu 2024-08-26 10:25:30
 **/
class MsgStoreTaskTest {

    @Mock
    DelayMsgDao delayDao;

    @Mock
    KafkaListener consumer;

    @InjectMocks
    MsgStoreTask msgStoreTask;

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"111}"
            , "{111}"
            , "{\"topic\":\"1\",\"messageKey\":\"1\",\"message\":\"1\",\"triggerTime\":0}"
            , "{\"topic\":\"\",\"messageKey\":\"1\",\"message\":\"1\",\"triggerTime\":9999999999999}"
            , "{\"topic\":null,\"messageKey\":\"1\",\"message\":\"1\",\"triggerTime\":1}"
            , "{\"topic\":\"1\",\"messageKey\":\"1\",\"message\":\"\",\"triggerTime\":1}"
            , "{\"topic\":\"1\",\"messageKey\":\"1\",\"message\":null,\"triggerTime\":1}"
    })
    void storeMsg_bad_msg(String msg) {
        List<ConsumerRecord<String, String>> records1 = new ArrayList<>();
        records1.add(new ConsumerRecord<>("1", 1, 0, "1", msg));
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        map.put(new TopicPartition("1", 1), records1);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(map);
        msgStoreTask.storeMsg(records);
        verify(delayDao, times(0)).batchStore(any(), anyList());
    }

    @Test
    void storeMsg() {
        List<ConsumerRecord<String, String>> records1 = new ArrayList<>();
        records1.add(new ConsumerRecord<>("1", 1, 0, "1",
                "{\"topic\":\"1\",\"messageKey\":\"1\",\"message\":\"1\",\"triggerTime\":1}"));
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        map.put(new TopicPartition("1", 1), records1);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(map);

        msgStoreTask.storeMsg(records);

        verify(delayDao, times(1)).batchStore(any(), anyList());
    }

    @Test
    void run() {
        msgStoreTask.run();
    }

    @Test
    void run_interrupt() {
        Thread.currentThread().interrupt();
        msgStoreTask.run();
    }

    @BeforeEach
    private void setup() {
        MockitoAnnotations.openMocks(this);
    }

}