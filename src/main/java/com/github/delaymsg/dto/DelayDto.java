package com.github.delaymsg.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author yhh 2021-12-19 22:17:59
 **/
public class DelayDto implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DelayDto.class);

    private static final long serialVersionUID = 1L;

    /**
     * 唯一id
     */
    private String id;

    /**
     * 实际业务topic
     */
    private String topic;

    /**
     * 消息的key
     */
    private String messageKey;

    /**
     * 待转发的消息【json字符串】
     */
    private String message;

    /**
     * 延时触发时间点.时间戳[精准到秒级别]
     */
    private Long triggerTime;

    public String getId() {
        return id;
    }

    @JsonIgnore
    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(Long triggerTime) {
        this.triggerTime = triggerTime;
    }

    public boolean checkFormat() {
        if (triggerTime == null || triggerTime <= 0L) {
            log.warn("triggerTime[{}] 异常", triggerTime);
            return false;
        }
        if (topic == null || topic.isEmpty()) {
            log.warn("topic 不能为空");
            return false;
        }
        if (message == null || message.isEmpty()) {
            log.warn("message 不能为空");
            return false;
        }
        return true;
    }

}
