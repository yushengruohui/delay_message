package com.yhh.utils;

import com.yhh.dto.DelayDto;
import org.nustaq.serialization.FSTConfiguration;

/**
 * <pre>
 * fst 是高效序列化工具类，简单封装下，主要避免内部创建的流竞争锁冲突。
 * 如果追求更高效率，可以预注册bean的class和禁用循环引用。
 * 注意事项：
 * 1. 序列化或者反序列化的类必须实现 Serializable 接口
 * 2. @Version 只能向后兼容类的字段变更
 * </pre>
 *
 * @author yhh 2023-04-11 19:41:40
 **/
public class FstUtils {

    /**
     * 为每个线程创建一个可复用的 fst，避免内部创建的流竞争锁冲突
     */
    private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
        FSTConfiguration configuration = FSTConfiguration.createDefaultConfiguration();
        configuration.registerClass(DelayDto.class);
        configuration.setShareReferences(false);
        return configuration;
    });

    private FstUtils() {
    }

    public static byte[] write(Object bean) {
        return getFst().asByteArray(bean);
    }

    public static <T> T read(byte[] bean) {
        //noinspection unchecked
        return (T) getFst().asObject(bean);
    }

    private static FSTConfiguration getFst() {
        return conf.get();
    }

}
