package com.wpf.kafka.data;

/**
 * Created by wenpengfei on 2016/7/4.
 */
public class KafkaMessage {

    private String topic;
    private String key;
    private String content;

    public KafkaMessage(String topic, String key, String content) {
        this.topic = topic;
        this.key = key;
        this.content = content;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
