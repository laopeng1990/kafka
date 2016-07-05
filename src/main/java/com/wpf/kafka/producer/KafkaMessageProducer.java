package com.wpf.kafka.producer;

import com.wpf.kafka.common.Consts;
import com.wpf.kafka.data.KafkaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by wenpengfei on 2016/7/4.
 */
public class KafkaMessageProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private KafkaProducer<String, String> producer;

    @SuppressWarnings("unchecked")
    public KafkaMessageProducer(Properties properties) {
        producer = new KafkaProducer<>(new HashMap<String, Object>((Map)properties),
                new StringSerializer(), new StringSerializer());
    }

    public void send(String topicName, KafkaMessage message) {
        try {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName, message.getKey(), message.getContent());
            producer.send(producerRecord, null);
        } catch (Exception e) {
            LOG.error("send", e);
        }
    }

    public static void main(String[] args) {
        KafkaMessageProducer producer = KafkaMessageProducerFactory.createProducer();
        int key = 1;
        Scanner sc = new Scanner(System.in);
        String line;
        LOG.info("please input:");
        while (sc.hasNextLine()) {
            line = sc.nextLine();
            KafkaMessage message = new KafkaMessage(Consts.TOPIC_NAME, String.valueOf(key++), line);
            producer.send(Consts.TOPIC_NAME, message);
        }
    }
}
