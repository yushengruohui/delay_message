package com.github.delaymsg.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author HuaHui Yu 2024-08-26 17:19:53
 **/
class KafkaSenderTest {

    @Mock
    KafkaProducer<String, String> kafkaProducer;

    @InjectMocks
    KafkaSender kafkaSender;

    @Test
    void of() {
        KafkaSender result = KafkaSender.of("127.0.0.1:9003");
        assertNotNull(result);
    }

    @Test
    void testSend() {
        kafkaSender.send("topic", "msgKey", "msg", (k, v) -> {

        });
        verify(kafkaProducer, times(1)).send(any(), any());
    }

    @BeforeEach
    private void setUp() {
        MockitoAnnotations.openMocks(this);
    }

}