package com.yhh.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yhh.constant.DelayConst;
import com.yhh.dto.DelayDto;
import com.yhh.utils.JsonUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yhh 2021-12-22 22:02:18
 **/
public class DelayDao {
    private static final Logger log = LoggerFactory.getLogger(DelayDao.class);
    private final RocksDB rocksDB;


    public DelayDao(int dbId) {
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
            throw new RuntimeException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(rocksDB::close));
    }

    /**
     * 扫描到达延期时限的消息
     *
     * @return
     */
    public List<DelayDto> scanTodoMsg() {
        // long start = System.currentTimeMillis();
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
                String v = new String(value1);
                DelayDto dto = JsonUtils.read(v, DelayDto.class);
                list.add(dto);
            }
        } catch (JsonProcessingException ignored) {
        }
        return list;
        // log.info("scanTodoMsg || 耗时: {} ms ", System.currentTimeMillis() - start);
    }

    /**
     * 批量存储延时消息到本地数据库
     *
     * @param qoList
     */
    public void batchStore(List<String> keys, List<String> values) {
        // long start = System.currentTimeMillis();
        try (
                WriteOptions writeOptions = new WriteOptions();
                WriteBatch writeBatch = new WriteBatch()
        ) {
            for (int i = 0, recordsSize = values.size(); i < recordsSize; i++) {
                String k = keys.get(i);
                String v = values.get(i);
                writeBatch.put(k.getBytes(), v.getBytes());
            }
            rocksDB.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            // 异常概率非常低，修改为不用检测的异常
            throw new RuntimeException(e);
        }
        // log.info("方法 : batchStore || 耗时: {} ms ", System.currentTimeMillis() - start);
    }

    /**
     * 批量删除本地延时消息
     *
     * @param idList 延时消息id
     */
    public void batchDelete(List<String> keys) {
        // long start = System.currentTimeMillis();
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
        // log.info("方法 : batchDelete || 耗时: {} ms ", System.currentTimeMillis() - start);
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
