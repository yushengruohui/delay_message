package com.github.delaymsg.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author HuaHui Yu 2024-08-26 16:52:29
 **/
class KafkaListenerTest {

    @SuppressWarnings("unchecked")
    KafkaConsumer<String, String> kafkaConsumer = mock(KafkaConsumer.class);

    KafkaListener kafkaListener = spy(new KafkaListener(kafkaConsumer));

    @Test
    void of() {
        KafkaListener listener = KafkaListener.of("127.0.0.1:9003", "delay_msg_app");
        assertNotNull(listener);
    }

    @Test
    void subscribe() {
        List<ConsumerRecord<String, String>> records1 = new ArrayList<>();
        records1.add(new ConsumerRecord<>("topicName", 1, 0, "1", "2222"));
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        map.put(new TopicPartition("topicName", 1), records1);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(map);
        doReturn(records).when(kafkaConsumer).poll(anyLong());

        kafkaListener.subscribe("topicName", (s) -> Thread.currentThread().interrupt());
        verify(kafkaConsumer, times(1)).commitAsync(any());
    }

}
