package org.example.scaningestionservice.entity;

public enum ScanStatus {
    RECEIVED,  // Получен сервисом, но еще не обработан консьюмером
    PROCESSING, // В процессе обработки консьюмером
    PROCESSED, // Успешно обработан и сохранен в БД
    ERROR,     // Произошла ошибка при обработке
    DUPLICATE  // Обнаружен как дубликат
}
