package com.github.delaymsg.dao;

import com.github.delaymsg.dto.DelayDto;
import com.github.delaymsg.utils.FstUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author HuaHui Yu 2024-08-26 17:32:16
 **/
class DelayMsgDaoTest {

    @Mock
    RocksDB rocksDB;

    @InjectMocks
    DelayMsgDao delayMsgDao;

    @Test
    void build() {
        DelayMsgDao dao = new DelayMsgDao(1);
        assertNotNull(dao);
    }

    @Test
    public void scanTodoMsg() {
        DelayDto dto = new DelayDto();

        RocksIterator iterator = Mockito.mock(RocksIterator.class);
        when(rocksDB.newIterator()).thenReturn(iterator);
        when(iterator.isValid()).thenReturn(true);
        when(iterator.key()).thenReturn("000".getBytes(StandardCharsets.UTF_8));
        when(iterator.value()).thenReturn(FstUtils.write(dto));

        List<DelayDto> result = delayMsgDao.scanTodoMsg();

        assertEquals(10000, result.size());
        assertEquals(dto.getMessageKey(), result.get(0).getMessageKey());
    }

    @Test
    public void testBatchStore() throws RocksDBException {
        List<String> keys = Arrays.asList("key1", "key2");
        List<byte[]> values = Arrays.asList("value1".getBytes(), "value2".getBytes());

        delayMsgDao.batchStore(keys, values);

        verify(rocksDB, times(1)).write(any(WriteOptions.class), any(WriteBatch.class));
    }

    @Test
    public void testBatchDelete() throws RocksDBException {
        List<String> keys = Arrays.asList("key1", "key2");

        delayMsgDao.batchDelete(keys);

        verify(rocksDB, times(1)).write(any(WriteOptions.class), any(WriteBatch.class));
    }

    @BeforeEach
    private void setUp() {
        MockitoAnnotations.openMocks(this);
    }

}
