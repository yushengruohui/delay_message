package com.yhh.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * 单例模式的JsonUtils工具类
 * 注意事项：
 * 如果bean不符合标准 Java Bean 的命名规则，转换会有问题
 * 解决方法：在实体类上加 @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY, getterVisibility=JsonAutoDetect.Visibility.NONE)
 *
 * @author yusheng 2020-04-14 00:29
 **/
public final class JsonUtils {

    private JsonUtils() {
    }

    private static final ObjectMapper OBJECT_MAPPER = initObjectMapper();

    /**
     * 把json字符串反序列化为对象
     *
     * @param jsonString
     * @param beanClass  [一定要有无参构造函数]
     * @return
     */
    public static <T> T read(String jsonString, Class<T> beanClass) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(jsonString, beanClass);
    }

    public static <T> String write(T bean) {
        try {
            return OBJECT_MAPPER.writeValueAsString(bean);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(bean.toString(), e);
        }
    }


    private static ObjectMapper initObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // json字符串中存在bean没有的属性，反序列化不抛出异常
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

}
