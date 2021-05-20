package com.pholema.tool.starter.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.pholema.tool.starter.kafka.util.KafkaConfigProperties;
import com.pholema.tool.utils.common.StringUtils;

public class KafkaInstance4Producer {

	private static Logger logger = Logger.getLogger(KafkaInstance4Producer.class);

	private static KafkaProducer<String, String> producer = null;

	public static void init(KafkaConfigProperties kafkaConfigProperties) {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaConfigProperties.getProperty("kafka.producer.props.bootstrap.servers"));
		props.put("acks", kafkaConfigProperties.getProperty("kafka.producer.props.acks"));
		props.put("retries", kafkaConfigProperties.getProperty("kafka.producer.props.retries"));
		props.put("batch.size", kafkaConfigProperties.getProperty("kafka.producer.props.batch.size"));
		props.put("linger.ms", kafkaConfigProperties.getProperty("kafka.producer.props.linger.ms"));
		props.put("buffer.memory", kafkaConfigProperties.getProperty("kafka.producer.props.buffer.memory"));
		// fixed KafkaProducer<String, String>
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		getInstance(props);
	}

	private static KafkaProducer getInstance(Properties properties) {
		if (producer == null) {
			producer = new KafkaProducer<>(properties);
		}
		return producer;
	}

	public static void send(Map<String, Object> map, String topic) {
		for (String key : map.keySet()) {
			send(map.get(key), topic);
		}
	}

	public static void send(Object obj, String topic) {
		String value = StringUtils.toGson(obj);
		ProducerRecord<String, String> msg = new ProducerRecord<>(topic, value);
		producer.send(msg);
	}
	
	public static void sendString(String message, String topic) {
		ProducerRecord<String, String> msg = new ProducerRecord<>(topic, message);
		producer.send(msg);
	}

	public static void flush() {
		producer.flush();
	}

}
