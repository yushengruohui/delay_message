package com.github.delaymsg.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 递增 id 生成工具类
 * 参考雪花算法的实现。
 * 由于是本地程序，不需要机器码[workerId]
 *
 * @author yusheng
 */
public final class IdUtils {

    private static final Logger log = LoggerFactory.getLogger(IdUtils.class);

    /**
     * 时钟回拨最大容错时间
     */
    private static final int CLOCK_ERROR_MAX_MS = 50;

    /**
     * 序列号
     */
    private static final long SEQUENCE_BITS = 14L;

    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS;

    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private static long sequence = 0L;

    private static long lastTimestamp = -1L;

    private IdUtils() {
    }

    /**
     * 根据雪花算法生成增势id
     *
     * @return 雪花算法生成的workerId
     */
    public static synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            // 时钟回拨处理
            long offset = lastTimestamp - timestamp;
            if (offset <= CLOCK_ERROR_MAX_MS) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(offset));
                timestamp = System.currentTimeMillis();
                if (timestamp < lastTimestamp) {
                    log.error("出现时间回拨，超过最大容忍时间也没有恢复正常。timestamp : {} || lastTimestamp : {}", timestamp, lastTimestamp);
                    SystemUtils.exit();
                }
            } else {
                log.error("出现时间回拨，超过最大容忍时间也没有恢复正常。timestamp : {} || lastTimestamp : {}", timestamp, lastTimestamp);
                SystemUtils.exit();
            }
        }
        if (lastTimestamp == timestamp) {
            // sequence = (sequence + 1) % (2^SEQUENCE_BITS);
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                timestamp = System.currentTimeMillis();
                while (timestamp <= lastTimestamp) {
                    timestamp = System.currentTimeMillis();
                }
            }
        } else {
            //如果是新的ms开始
            sequence = 0;
        }
        lastTimestamp = timestamp;
        return (timestamp << TIMESTAMP_LEFT_SHIFT) | sequence;
    }

}
