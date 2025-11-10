package org.example.scaningestionservice.repository;

import org.example.scaningestionservice.entity.Scan;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ScanRepository extends JpaRepository<Scan, Long> {
    Optional<Scan> findByScanId(String scanId);
    boolean existsByScanId(String scanId);
}
