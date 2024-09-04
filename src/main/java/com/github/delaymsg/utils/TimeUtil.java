package com.github.delaymsg.utils;

/**
 * @author HuaHui Yu 2024-09-04 15:58:41
 **/
public class TimeUtil {

    private static final String KEY_SPLIT_CHAR = "#";

    public static long unixTime() {
        return System.currentTimeMillis() / 1000;
    }

    public static long extractTriggerTime(String key) {
        String[] split = key.split(KEY_SPLIT_CHAR);
        return Long.parseLong(split[0]);
    }

    public static String buildTriggerKey(long triggerTime) {
        return triggerTime + KEY_SPLIT_CHAR + IdUtils.nextId();
    }

}
