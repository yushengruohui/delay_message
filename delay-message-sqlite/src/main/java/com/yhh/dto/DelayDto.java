package com.yhh.dto;

/**
 * @author yhh 2021-12-19 22:17:59
 **/
public class DelayDto {
    /**
     * 唯一id
     */
    private Long id;
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
     * 延时时间戳[精准到秒级别]
     */
    private Integer delayTime;

    @Override
    public String toString() {
        return "DelayDto{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", messageKey='" + messageKey + '\'' +
                ", message='" + message + '\'' +
                ", delayTime=" + delayTime +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
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

    public Integer getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Integer delayTime) {
        this.delayTime = delayTime;
    }
}
