# KafkaStarter
## Overview
help you to implement Kafka consumer and producer quickly.

## Description
### kafka properties:
    refer https://github.com/pholema/KafkaStarter/blob/master/src/main/resources/kafka.properties.example

### Consumer:
    kafkaConfigProperties = KafkaConfigProperties.load(PropertiesUtils.properties.getProperty("kafka.configuration"));
            for (String node : kafkaConfigProperties.getConsumerNodeList()) {
                KafkaInstance instance = new KafkaInstance(kafkaConfigProperties, node)
                        .addConsumerRecordHandler(CustomerProfile.register());
                instance.start();
            }

### Producer:
    kafkaConfigProperties = KafkaConfigProperties.load(PropertiesUtils.properties.getProperty("kafka.configuration"));
    KafkaInstance4Producer.init(kafkaConfigProperties);
