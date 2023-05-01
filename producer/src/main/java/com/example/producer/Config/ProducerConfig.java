package com.example.producer.Config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfig {
    private final KafkaProperties kafkaProperties;

    @Value("${topic.name}")
    String topicName;

    @Autowired
    public ProducerConfig(KafkaProperties kafkaProperties) {

        this.kafkaProperties = kafkaProperties;
    }

    //creating Producer Object using Factory Methods..
    @Bean
    public ProducerFactory<String,String> producerFactory()
    {
        //get the producer properties from application.properties / application.yml file
        // else we need to define a Map for configs and pass that configs in DefaultkafkaProducerFact
        //buildProducerProperties() - https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/kafka/KafkaProperties.html
        var producerProps = kafkaProperties.buildProducerProperties();
        // DefaultKafkaProducerFactory -  https://docs.spring.io/spring-kafka/api/org/springframework/kafka/core/DefaultKafkaProducerFactory.html
        return  new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate()
    {
        //kafkaTemplate() - > https://docs.spring.io/spring-kafka/api/org/springframework/kafka/core/KafkaTemplate.html
        return new KafkaTemplate<>(producerFactory());
    }
    @Bean
    public NewTopic topic()
    {
        Map<String,String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", Long.toString(86400000));

        topicConfigs.put("retention.bytes", Integer.toString(1024*1024));
        return TopicBuilder.
                name(topicName)
                .partitions(3)
                .replicas(1)
                .configs(topicConfigs)
                .build();
    }

}
