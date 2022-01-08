package com.yhh.utils;

/**
 * @author yhh 2021-12-23 23:32:32
 **/
public final class ExitUtils {
    private ExitUtils() {
    }

    /**
     * 系统异常退出
     */
    public static void exit() {
        // todo 发送短信通知
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException ignored) {
        }
        System.exit(-1);
    }

    /**
     * 系统异常退出
     */
    public static void exit(Throwable e) {
        // todo 发送短信通知
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException ignored) {
        }
        System.exit(-1);
    }
}
