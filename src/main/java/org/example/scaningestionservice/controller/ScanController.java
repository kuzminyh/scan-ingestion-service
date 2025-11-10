package org.example.scaningestionservice.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.scaningestionservice.dto.ScanEventDto;
import org.example.scaningestionservice.service.ScanIngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/scans")
@RequiredArgsConstructor
@Slf4j
public class ScanController {

    private final ScanIngestionService scanIngestionService;

    @PostMapping("/ingest")
    public ResponseEntity<String> ingestScan(@Valid @RequestBody ScanEventDto scanEventDto) {
        log.info("Received scan ingestion request for scanId: {}", scanEventDto.getScanId());
        scanIngestionService.ingestScanEvent(scanEventDto);
        return ResponseEntity.accepted().body("Scan event for " + scanEventDto.getScanId() + " accepted for processing.");
    }
}
