package org.example.scaningestionservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Value;
import org.apache.kafka.clients.admin.NewTopic;
import org.example.scaningestionservice.dto.ScanEventDto;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic}")
    private String topicName;

    @Value("${app.kafka.partitions}")
    private int numPartitions;

    @Value("${app.kafka.replication-factor}")
    private short replicationFactor;

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(topicName, numPartitions, replicationFactor);
    }

    // Конфигурация ObjectMapper для корректной сериализации/десериализации LocalDateTime
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }

    // Конфигурация для консьюмера с ручным подтверждением Acknowledgment
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ScanEventDto> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, ScanEventDto> kafkaConsumerFactory,
            KafkaTemplate<String, ScanEventDto> kafkaTemplate // Можно инжектить для DLQ, если есть
    ) {
        ConcurrentKafkaListenerContainerFactory<String, ScanEventDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE); // Ручное подтверждение
        // Можно настроить Dead Letter Queue (DLQ) здесь, если нужно
        // factory.setRecoveryCallback(new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> new TopicPartition("dlq-topic", r.partition())));
        return factory;
    }
}