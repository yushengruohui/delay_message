package com.github.delaymsg.dao;

import com.github.delaymsg.constant.DelayConst;
import com.github.delaymsg.dto.DelayDto;
import com.github.delaymsg.utils.FstUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yhh 2021-12-22 22:02:18
 **/
public class DelayMsgDao {

    private static final Logger log = LoggerFactory.getLogger(DelayMsgDao.class);

    private final RocksDB rocksDB;

    public DelayMsgDao(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    public DelayMsgDao(int dbId) {
        String dbDir = DelayConst.STORE_PATH + "/" + dbId;
        File dir = new File(dbDir);
        dir.mkdirs();
        RocksDB.loadLibrary();
        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            // 根据业务场景，优化数据库配置
            rocksDB = RocksDB.open(options, dbDir);
        } catch (RocksDBException e) {
            log.error("RocksDbHelper[初始化失败] || dbDir : {} ", dbDir, e);
            throw new IllegalStateException("Failed to initialize RocksDB at path: " + dbDir, e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(rocksDB::close));
    }

    /**
     * 扫描到达延期时限的消息
     *
     * @return
     */
    public List<DelayDto> scanTodoMsg() {
        byte[] bytes = String.valueOf(System.currentTimeMillis() / 1000).getBytes();
        List<DelayDto> list = new ArrayList<>();
        try (RocksIterator iterator = rocksDB.newIterator()) {
            int i = 0;
            for (iterator.seekToFirst(); iterator.isValid() && i < 10000; iterator.next(), i++) {
                byte[] key1 = iterator.key();
                if (ByteArraysCompare(key1, bytes) > 0) {
                    break;
                }
                byte[] value1 = iterator.value();
                DelayDto dto = FstUtils.read(value1);
                list.add(dto);
            }
        }
        return list;
    }

    /**
     * 批量存储延时消息到本地数据库
     */
    public void batchStore(List<String> keys, List<byte[]> values) {
        try (
                WriteOptions writeOptions = new WriteOptions();
                WriteBatch writeBatch = new WriteBatch()
        ) {
            for (int i = 0, recordsSize = values.size(); i < recordsSize; i++) {
                String k = keys.get(i);
                writeBatch.put(k.getBytes(), values.get(i));
            }
            rocksDB.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            // 异常概率非常低，修改为不用检测的异常
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量删除本地延时消息
     */
    public void batchDelete(List<String> keys) {
        try (
                WriteOptions writeOptions = new WriteOptions();
                WriteBatch writeBatch = new WriteBatch()
        ) {
            for (String key : keys) {
                writeBatch.delete(key.getBytes());
            }
            rocksDB.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            // 异常概率非常低，修改为不用检测的异常
            throw new RuntimeException(e);
        }
    }

    private static int ByteArraysCompare(byte[] x, byte[] y) {
        int length1 = x.length;
        int length2 = y.length;
        int maxLength = Math.min(length1, length2);
        for (int i = 0; i < maxLength; i++) {
            byte b1Byte = x[i];
            byte b2Byte = y[i];
            if (b1Byte != b2Byte) {
                return Byte.compare(b1Byte, b2Byte);
            }
        }
        return length1 - length2;
    }

}
