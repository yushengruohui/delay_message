package com.github.delaymsg.task;

import com.github.delaymsg.dao.DelayMsgDao;
import com.github.delaymsg.dto.DelayDto;
import com.github.delaymsg.kafka.KafkaSender;
import org.apache.kafka.clients.producer.Callback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author HuaHui Yu 2024-08-26 11:26:33
 **/
class MsgTransferTaskTest {

    @Mock
    KafkaSender kafkaSender;

    @Mock
    DelayMsgDao delayDao;

    @InjectMocks
    MsgTransferTask msgTransferTask;

    @Test
    void transfer_empty_msg() {
        Mockito.doReturn(new ArrayList<>())
                .when(delayDao)
                .scanTodoMsg();

        msgTransferTask.scanAndTransfer();

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
        message.setTriggerTime(2L);
        List<DelayDto> messages = new ArrayList<>();
        messages.add(message);
        when(delayDao.scanTodoMsg()).thenReturn(messages);

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(3);
            callback.onCompletion(null, null);
            return null;
        }).when(kafkaSender).send(any(), any(), any(), any());

        msgTransferTask.scanAndTransfer();

        verify(delayDao, times(1)).batchDelete(anyList());
    }

    @BeforeEach
    private void setUp() {
        MockitoAnnotations.openMocks(this);
    }

}
