package com.github.delaymsg.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


/**
 * 单例模式的JsonUtils工具类
 * 注意事项：
 * 如果bean不符合标准 Java Bean 的命名规则，转换会有问题
 * 解决方法：在实体类上加 @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY, getterVisibility=JsonAutoDetect.Visibility.NONE)
 *
 * @author yusheng 2020-04-14 00:29
 **/
public final class JsonUtils {

    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper OBJECT_MAPPER = initObjectMapper();

    private JsonUtils() {
    }

    /**
     * 把json字符串反序列化为对象
     *
     * @param jsonString
     * @param beanClass  [一定要有无参构造函数]
     * @return
     */
    public static <T> Optional<T> read(String jsonString, Class<T> beanClass) {
        try {
            T readValue = OBJECT_MAPPER.readValue(jsonString, beanClass);
            return Optional.ofNullable(readValue);
        } catch (JsonProcessingException e) {
            log.warn("json 反序列化失败，jsonString: {}", jsonString, e);
        }
        return Optional.empty();
    }

    private static ObjectMapper initObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // json字符串中存在bean没有的属性，反序列化不抛出异常
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

}
