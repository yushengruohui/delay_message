package com.yhh.utils;

/**
 * @author yhh 2021-12-23 23:32:32
 **/
public final class SystemUtils {
    private SystemUtils() {
    }

    /**
     * 系统异常退出
     */
    public static void exit() {
        // todo 发送短信通知
        sleep(1000L);
        System.exit(-1);
    }

    /**
     * 让线程休眠
     *
     * @param timeout 休眠时间，单位毫秒
     */
    public static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
