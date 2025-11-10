package org.example.scaningestionservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.scaningestionservice.dto.ScanEventDto;
import org.example.scaningestionservice.entity.Scan;
import org.example.scaningestionservice.entity.ScanStatus;
import org.example.scaningestionservice.repository.ScanRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final ScanRepository scanRepository;
    private final StringRedisTemplate redisTemplate; // Для кэша дубликатов и мастер-данных
    private final ObjectMapper objectMapper; // Для сериализации/десериализации в Redis

    // Ключ для Redis для отслеживания обработанных сканов (предотвращение дубликатов)
    private static final String REDIS_SCAN_PROCESSED_KEY_PREFIX = "scan:processed:";
    private static final Duration REDIS_SCAN_PROCESSED_TTL = Duration.ofHours(24); // Хранить 24 часа

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    @Transactional // Транзакция для сохранения в БД
    public void listen(ScanEventDto scanEventDto, Acknowledgment acknowledgment) {
        log.info("Received message from Kafka for scanId: {}", scanEventDto.getScanId());

        try {
            // 1. Проверка на дубликаты (Redis + DB)
            if (isScanProcessed(scanEventDto.getScanId())) {
                log.warn("Scan with scanId: {} already processed or is a duplicate. Skipping.", scanEventDto.getScanId());
                markScanAsDuplicateInDb(scanEventDto); // Записываем в БД как дубликат
                acknowledgment.acknowledge(); // Подтверждаем обработку сообщения
                return;
            }

            // 2. Валидация данных (можно добавить более сложную бизнес-валидацию)
            // Здесь предполагается, что DTO уже прошла JSR-303 валидацию на уровне REST.
            // Но можно добавить дополнительную, например, проверку существования товара по баркоду.
            validateScanEvent(scanEventDto);

            // 3. Сохранение скана в базу данных
            Scan scan = mapDtoToEntity(scanEventDto);
            scan.setStatus(ScanStatus.PROCESSING); // Устанавливаем статус PROCESSING
            scan.setIngestionTimestamp(LocalDateTime.now()); // Время обработки консьюмером

            Scan savedScan = scanRepository.save(scan);
            log.info("Saved scan with ID: {} and scanId: {}", savedScan.getId(), savedScan.getScanId());

            // 4. Дополнительная логика (например, обновление запасов, отправка в другие системы)
            // ... (здесь может быть вызов других сервисов)

            // 5. Помечаем скан как обработанный в Redis
            markScanAsProcessed(scanEventDto.getScanId());

            // 6. Обновляем статус в БД на PROCESSED
            savedScan.setStatus(ScanStatus.PROCESSED);
            scanRepository.save(savedScan);

            log.info("Successfully processed scan event for scanId: {}", scanEventDto.getScanId());
            acknowledgment.acknowledge(); // Подтверждаем успешную обработку сообщения
        } catch (Exception e) {
            log.error("Error processing scan event for scanId: {}: {}", scanEventDto.getScanId(), e.getMessage(), e);
            // В случае ошибки, можно сохранить скан со статусом ERROR
            markScanAsErrorInDb(scanEventDto, e.getMessage());
            // Не подтверждаем сообщение, чтобы оно было повторно обработано (если не сконфигурирован DLQ)
            // Либо, если настроен Dead Letter Queue (DLQ), отправляем туда.
            // Для простоты, здесь мы полагаемся на то, что Kafka перешлет сообщение после max.poll.records
            // или если консьюмер упадет. Для продакшена, лучше использовать DLQ.
            // acknowledgment.acknowledge(); // Если хотим подтвердить и не переобрабатывать (но это может привести к потере)
        }
    }

    private boolean isScanProcessed(String scanId) {
        // Сначала проверяем в Redis
        if (redisTemplate.hasKey(REDIS_SCAN_PROCESSED_KEY_PREFIX + scanId)) {
            log.debug("ScanId {} found in Redis as already processed.", scanId);
            return true;
        }

        // Если нет в Redis, проверяем в основной БД
        boolean existsInDb = scanRepository.existsByScanId(scanId);
        if (existsInDb) {
            // Если нашли в БД, добавляем в Redis, чтобы избежать повторных запросов к БД
            redisTemplate.opsForValue().set(REDIS_SCAN_PROCESSED_KEY_PREFIX + scanId, "true", REDIS_SCAN_PROCESSED_TTL);
            log.debug("ScanId {} found in DB as already processed. Added to Redis.", scanId);
        }
        return existsInDb;
    }

    private void markScanAsProcessed(String scanId) {
        redisTemplate.opsForValue().set(REDIS_SCAN_PROCESSED_KEY_PREFIX + scanId, "true", REDIS_SCAN_PROCESSED_TTL);
    }

    private void validateScanEvent(ScanEventDto scanEventDto) {
        // Здесь можно добавить более сложную бизнес-валидацию,
        // например, проверку существования товара в системе по barcode
        // или проверку валидности locationCode.
        // Пример:
        // if (!productService.existsByBarcode(scanEventDto.getBarcode())) {
        //     throw new IllegalArgumentException("Product with barcode " + scanEventDto.getBarcode() + " not found.");
        // }
    }

    private Scan mapDtoToEntity(ScanEventDto dto) {
        Scan scan = new Scan();
        scan.setScanId(dto.getScanId());
        scan.setBarcode(dto.getBarcode());
        scan.setDeviceId(dto.getDeviceId());
        scan.setScanTimestamp(dto.getScanTimestamp());
        scan.setQuantity(dto.getQuantity());
        scan.setLocationCode(dto.getLocationCode());
        scan.setScanType(dto.getScanType());
        // IngestionTimestamp и Status будут установлены позже
        return scan;
    }

    private void markScanAsErrorInDb(ScanEventDto scanEventDto, String errorMessage) {
        // Попытка найти скан, если он уже был создан в базе со статусом RECEIVED
        Optional<Scan> existingScan = scanRepository.findByScanId(scanEventDto.getScanId());
        Scan scan;
        if (existingScan.isPresent()) {
            scan = existingScan.get();
        } else {
            // Если скан еще не был сохранен, создаем новую запись
            scan = mapDtoToEntity(scanEventDto);
            scan.setIngestionTimestamp(LocalDateTime.now());
        }
        scan.setStatus(ScanStatus.ERROR);
        scan.setErrorMessage(errorMessage.length() > 255 ? errorMessage.substring(0, 255) : errorMessage); // Обрезаем сообщение об ошибке
        try {
            scanRepository.save(scan);
        } catch (Exception e) {
            log.error("Failed to save scan with error status for scanId: {}: {}", scanEventDto.getScanId(), e.getMessage());
        }
    }

    private void markScanAsDuplicateInDb(ScanEventDto scanEventDto) {
        Optional<Scan> existingScan = scanRepository.findByScanId(scanEventDto.getScanId());
        if (existingScan.isPresent()) {
            Scan scan = existingScan.get();
            if (scan.getStatus() != ScanStatus.DUPLICATE) { // Избегаем повторной записи, если уже дубликат
                scan.setStatus(ScanStatus.DUPLICATE);
                scan.setErrorMessage("Duplicate scan detected.");
                try {
                    scanRepository.save(scan);
                } catch (Exception e) {
                    log.error("Failed to update scan as duplicate for scanId: {}: {}", scanEventDto.getScanId(), e.getMessage());
                }
            }
        } else {
            // Если дубликат пришел, но его нет в БД (например, был удален или Redis устарел),
            // можно его просто игнорировать или создать запись со статусом DUPLICATE
            Scan scan = mapDtoToEntity(scanEventDto);
            scan.setIngestionTimestamp(LocalDateTime.now());
            scan.setStatus(ScanStatus.DUPLICATE);
            scan.setErrorMessage("Duplicate scan detected. Original might have been processed previously.");
            try {
                scanRepository.save(scan);
            } catch (Exception e) {
                log.error("Failed to save new scan as duplicate for scanId: {}: {}", scanEventDto.getScanId(), e.getMessage());
            }
        }
    }
}
