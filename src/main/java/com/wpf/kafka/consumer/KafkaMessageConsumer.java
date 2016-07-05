package com.wpf.kafka.consumer;

import com.google.common.collect.ImmutableMap;
import com.wpf.kafka.data.KafkaMessage;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.wpf.kafka.common.Consts.*;

/**
 * Created by wenpengfei on 2016/7/4.
 */
public class KafkaMessageConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private String topicName;
    private ConsumerConnector connector = null;
    private int partitionNum = 1;

    ConsumerIterator<byte[], byte[]> iterator = null;

    public KafkaMessageConsumer(String topicName, ConsumerConnector consumerConnector, int partitionNum) {
        this.connector = consumerConnector;
        this.topicName = topicName;
        this.partitionNum = partitionNum;
        startUp();
    }

    private void startUp() {
        Map<String, Integer> consumerCntMap = ImmutableMap.of(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(consumerCntMap);

        iterator = consumerMap.get(topicName).get(0).iterator();
    }

    public KafkaMessage next() {
        try {
            if (!iterator.hasNext()) {
                return null;
            }

            MessageAndMetadata<byte[], byte[]> metaMessage = iterator.next();
            String topic = metaMessage.topic();
            int partition = metaMessage.partition();
            String key = "";
            if (metaMessage.key() != null) {
                key = new String(metaMessage.key(), Charset.forName("utf8"));
            }
            String content = "";
            if (metaMessage.message() != null) {
                content = new String(metaMessage.message(), Charset.forName("utf8"));
            }

            LOG.info("topic {} partition {} key {} content {}", topic, partition, key, content);

            return new KafkaMessage(topic, key, content);
        } catch (ConsumerTimeoutException e) {
        } catch (Exception e) {
            LOG.error("next", e);
        }

        return null;
    }

    public static void main(String[] args) {
        KafkaMessageConsumer consumer = KafkaMessageConsumerFactory.createConsumer(TOPIC_NAME, GROUP_NAME);
        while (true) {
            KafkaMessage message = consumer.next();
        }
    }
}
