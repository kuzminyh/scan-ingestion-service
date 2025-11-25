package org.example.scaningestionservice.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "scans")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Scan {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String scanId; // Уникальный идентификатор скана (может быть GUID с ТСД)

    @Column(nullable = false)
    private String barcode;

    @Column(nullable = false)
    private String deviceId; // ID ТСД, с которого пришел скан

    @Column(nullable = false)
    private LocalDateTime scanTimestamp; // Время сканирования на ТСД

    private Integer quantity; // Количество, если скан включает количество

    private String locationCode; // Местоположение (например, склад, полка)

    @Enumerated(EnumType.STRING)
    private ScanType scanType; // Тип скана (INVENTORY, RECEIPT, SHIPMENT и т.д.)

    @Column(nullable = false)
    private LocalDateTime ingestionTimestamp; // Время приема сервисом

    @Enumerated(EnumType.STRING)
    private ScanStatus status; // Статус обработки (RECEIVED, PROCESSED, ERROR)

    private String errorMessage; // Сообщение об ошибке, если есть
}

