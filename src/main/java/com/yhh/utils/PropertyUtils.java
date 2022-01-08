package com.yhh.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * 读取properties文件配置工具类
 *
 * @author yhh 2021-12-21 12:24:27
 **/
public final class PropertyUtils {

    private PropertyUtils() {
    }

    private static final Logger log = LoggerFactory.getLogger(PropertyUtils.class);
    private static final String CLASSPATH = initClasspath();

    /**
     * 获取配置文件的指定配置
     *
     * @param configFile   classpath目录下的配置文件[eg: biz/biz.properties]
     * @param key          配置项
     * @param defaultValue 如果配置项不存在则返回指定配置
     * @return
     */
    public static String getConfig(String configFile, String key, String defaultValue) {
        File file = new File(CLASSPATH + configFile);
        InputStream resource = null;
        if (file.exists()) {
            try {
                resource = new FileInputStream(file);
            } catch (FileNotFoundException ignored) {
            }
        } else {
            resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
        }
        try {
            Properties properties = new Properties();
            properties.load(resource);
            String value = properties.getProperty(key, defaultValue);
            log.info("configFile : {} ||key : {} || value :{}", configFile, key, value);
            return value;
        } catch (IOException e) {
            log.error("getConfig[读取配置项异常] || configFile : {} ", configFile, e);
            ExitUtils.exit();
        } finally {
            if (resource != null) {
                try {
                    resource.close();
                } catch (IOException ignored) {
                }
            }
        }
        return defaultValue;
    }

    private static String initClasspath() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("");
        String path = "";
        if (resource == null) {
            log.error("initClasspath[加载classpath失败]，请检查 jar 包内的 MANIFEST.MF 配置");
            ExitUtils.exit();
        } else {
            path = resource.getPath();
        }
        if (path.length() > 2 & path.charAt(0) == '/' && path.charAt(2) == ':') {
            // 解决 window 系统路径无法识别问题
            return path.substring(1);
        }
        return path;
    }
}
