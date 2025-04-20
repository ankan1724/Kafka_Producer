package com.kafka.producer.KafkaProducer.service;

import org.springframework.kafka.support.serializer.JsonSerializer;

import com.kafka.producer.KafkaProducer.PaymentMessages;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.messaging.Message;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Config {

    @Bean
    public ProducerFactory<String, Object> plainProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean(name = "nonsslKafkaTemplate")
    public KafkaTemplate<String, Object> plainKafkaTemplate() {
        return new KafkaTemplate<>(plainProducerFactory());
    }

    // SSL Producer (9093)
    @Bean
    public ProducerFactory<String, Message<PaymentMessages>> sslProducerFactory(KafkaSSLProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "1");

        config.put("security.protocol", props.getSecurityProtocol());
        config.put("ssl.truststore.location", props.getTruststoreLocation());
        config.put("ssl.truststore.password", props.getTruststorePassword());

        config.put("ssl.key.password", props.getKeyPassword());
        config.put("ssl.endpoint.identification.algorithm", "");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean(name = "sslKafkaTemplate")
    @Primary
    public KafkaTemplate<String, Message<PaymentMessages>> sslKafkaTemplate(KafkaSSLProperties props) {
        ProducerFactory<String, Message<PaymentMessages>> producerFactory = sslProducerFactory(props);
        return new KafkaTemplate<>(producerFactory);
    }
}


