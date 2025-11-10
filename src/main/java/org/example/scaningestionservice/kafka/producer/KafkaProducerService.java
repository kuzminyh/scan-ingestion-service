package org.example.scaningestionservice.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.example.scaningestionservice.dto.ScanEventDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    @Value("${app.kafka.topic}")
    private String topicName;

    private final KafkaTemplate<String, ScanEventDto> kafkaTemplate;

    public void sendScanEvent(ScanEventDto scanEventDto) {
        log.info("Sending ScanEventDto with scanId: {} to Kafka topic: {}", scanEventDto.getScanId(), topicName);
        CompletableFuture<SendResult<String, ScanEventDto>> future = kafkaTemplate.send(topicName, scanEventDto.getScanId(), scanEventDto);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message with scanId=[{}] to partition=[{}] with offset=[{}]",
                        scanEventDto.getScanId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message with scanId=[{}] due to: {}", scanEventDto.getScanId(), ex.getMessage());
            }
        });
    }
}
