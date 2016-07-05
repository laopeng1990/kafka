package com.wpf.kafka.producer;

import java.util.Properties;

/**
 * Created by wenpengfei on 2016/7/4.
 */
public class KafkaMessageProducerFactory {

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.28.48.63:9092,172.28.48.63:9093,172.28.48.63:9094");
        props.put("send.buffer.bytes", 131072);
        props.put("retries", 1);

        return props;
    }

    public static KafkaMessageProducer createProducer() {
        KafkaMessageProducer producer = new KafkaMessageProducer(getProperties());
        return producer;
    }
}
