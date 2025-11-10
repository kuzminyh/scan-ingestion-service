package org.example.scaningestionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.scaningestionservice.dto.ScanEventDto;
import org.example.scaningestionservice.kafka.producer.KafkaProducerService;
import org.example.scaningestionservice.repository.ScanRepository;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScanIngestionService {

    private final KafkaProducerService kafkaProducerService;
    private final ScanRepository scanRepository; // Для первичной проверки дубликатов на уровне REST, если ТСД отправит быстро повторно

    // @Transactional(readOnly = true) // Можно сделать readOnly, т.к. только проверяем
    public void ingestScanEvent(ScanEventDto scanEventDto) {
        log.info("Received scan event for ingestion: {}", scanEventDto.getScanId());

        // Простая проверка на дубликаты на этапе приема, чтобы отсеять совсем "быстрые" дубликаты
        // Основная проверка будет в консьюмере Kafka
        if (scanRepository.existsByScanId(scanEventDto.getScanId())) {
            log.warn("Duplicate scanId received from REST endpoint: {}. Ignoring for now, but main check is in Kafka consumer.", scanEventDto.getScanId());
            // Можно вернуть 202 Accepted, но с предупреждением или 200 OK
            // В данном случае просто пропустим, т.к. Kafka консьюмер обработает это надежнее.
            // Или можно бросить исключение и вернуть 409 Conflict. Зависит от требований.
        }

        // Отправляем событие в Kafka для асинхронной обработки
        kafkaProducerService.sendScanEvent(scanEventDto);
        log.info("Scan event with scanId: {} published to Kafka.", scanEventDto.getScanId());
    }
}




