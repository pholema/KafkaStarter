package com.pholema.tool.starter.kafka.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.pholema.tool.utils.dynamic.CustomizeProperties;

public class KafkaConfigProperties extends CustomizeProperties {

    private static KafkaConfigProperties kafkaConfigProperties;

    private List<String> consumerNodeList;
    private List<String> consumerPropList;

    public List<String> getConsumerNodeList() {
        if (consumerNodeList == null) {
            consumerNodeList = Arrays.asList(kafkaConfigProperties.getProperty("kafka.consumer.servers.name").split(","));
        }
        return consumerNodeList;
    }

    private List<String> getConsumerPropList() {
        if (consumerPropList == null) {
            consumerPropList = Arrays.asList(kafkaConfigProperties.getProperty("kafka.consumer.props").split(","));
        }
        return consumerPropList;
    }

    public java.util.Properties getConsumerProperties(String node) {
        java.util.Properties properties = new java.util.Properties();
        for (String prop: getConsumerPropList()) {
            String propName = "kafka.consumer.props." + prop;
            if (prop.equals("bootstrap.servers") || prop.equals("zookeeper.connect")) {
                propName = propName + "." + node;
            }
            System.out.println("propName " + propName);
            properties.put(prop, kafkaConfigProperties.getProperty(propName));
        }
        return properties;
    }

    public static KafkaConfigProperties load(String configUrl) throws IOException {
        if (kafkaConfigProperties == null) {
            System.out.println("kafka.properties=" + configUrl);
            kafkaConfigProperties = new KafkaConfigProperties();
            kafkaConfigProperties.load(new FileInputStream(configUrl));
        }
        return kafkaConfigProperties;
    }
}
