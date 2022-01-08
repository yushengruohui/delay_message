package com.yhh.utils;

/**
 * 时间处理工具
 **/
public final class DateUtils {
    private DateUtils() {
    }

    /**
     * 获取秒级别时间戳
     *
     * @return
     */
    public static long epochSecond() {
        return System.currentTimeMillis() / 1000L;
    }
}
