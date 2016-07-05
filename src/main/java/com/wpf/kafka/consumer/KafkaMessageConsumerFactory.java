package com.wpf.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by wenpengfei on 2016/7/4.
 */
public class KafkaMessageConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumerFactory.class);

    public static KafkaMessageConsumer createConsumer(String topicName, String groupName) {
        return createConsumer(topicName, createConsumerConfig(groupName), 1);
    }

    public static KafkaMessageConsumer createConsumer(String topicName, String groupName, int partition) {
        return createConsumer(topicName, createConsumerConfig(groupName), partition);
    }

    private static KafkaMessageConsumer createConsumer(String topicName, ConsumerConfig consumerConfig, int partitionNum) {
        try {
            return new KafkaMessageConsumer(topicName, Consumer.createJavaConsumerConnector(consumerConfig), partitionNum);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static ConsumerConfig createConsumerConfig(String groupName) {
        Properties props = new Properties();
        props.put("group.id", groupName);
        props.put("zookeeper.connect", "172.28.48.63:2181");
        props.put("zookeeper.connect.timeout.ms", "6000");
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "6000");
        props.put("auto.offset.reset", "largest");
        props.put("offsets.storage", "kafka");
        props.put("consumer.timeout.ms", "2000");
        props.put("dual.commit.enabled", "true");
        props.put("auto.commit.interval.ms", "30000");
        return new ConsumerConfig(props);
    }
}
