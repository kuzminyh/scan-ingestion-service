package org.example.scaningestionservice.dto;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.scaningestionservice.entity.ScanType;

import java.time.LocalDateTime;

@Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class ScanEventDto {
        @NotBlank(message = "Scan ID cannot be blank")
        private String scanId; // Уникальный ID скана с ТСД

        @NotBlank(message = "Barcode cannot be blank")
        private String barcode;

        @NotBlank(message = "Device ID cannot be blank")
        private String deviceId;

        @NotNull(message = "Scan timestamp cannot be null")
        @PastOrPresent(message = "Scan timestamp must be in the past or present")
        private LocalDateTime scanTimestamp;

        @PositiveOrZero(message = "Quantity must be positive or zero")
        private Integer quantity = 1; // Дефолтное количество 1

        private String locationCode;

        @NotNull(message = "Scan type cannot be null")
        private ScanType scanType;
    }

