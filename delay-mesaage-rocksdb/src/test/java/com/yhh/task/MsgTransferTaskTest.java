package com.yhh.task;

import com.yhh.dao.DelayMsgDao;
import com.yhh.dto.DelayDto;
import com.yhh.utils.kafka.KafkaSender;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author HuaHui Yu 2024-08-26 11:26:33
 **/
class MsgTransferTaskTest {

    @Mock
    KafkaSender kafkaSender;

    @Mock
    KafkaProducer<String, String> kafkaProducer;

    @Mock
    DelayMsgDao delayDao;

    @Spy
    @InjectMocks
    MsgTransferTask msgTransferTask;

    @Test
    void transfer_empty_msg() {
        // 运行任务
        msgTransferTask.transfer(Collections.emptyList(), new CountDownLatch(0), new ArrayList<>(Collections.emptyList().size()));

        // 验证没有调用 kafkaSender
        verify(kafkaSender, never()).send(anyString(), anyString(), anyString(), any());
        verify(delayDao, never()).batchDelete(anyList());
    }

    @Test
    void transfer() {
        DelayDto message = new DelayDto();
        message.setId("11");
        message.setTopic("11");
        message.setMessageKey("22");
        message.setMessage("122");
        message.setDelayTime(2L);
        List<DelayDto> messages = new ArrayList<>();
        messages.add(message);
        when(delayDao.scanTodoMsg()).thenReturn(messages);

        // 运行任务
        List<String> toDeletedIds = new ArrayList<>(messages.size());
        toDeletedIds.add(message.getId());
        msgTransferTask.transfer(messages, new CountDownLatch(0), toDeletedIds);

        // 验证调用了发送和删除
        verify(kafkaSender, times(1)).send(anyString(), anyString(), anyString(), any());
    }

    @BeforeEach
    private void setUp() {
        MockitoAnnotations.openMocks(this);
    }

}
