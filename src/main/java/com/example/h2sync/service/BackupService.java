package com.example.h2sync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class BackupService {
    private static final Logger log = LoggerFactory.getLogger(BackupService.class);
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public BackupService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public File backupTo(String targetDir, String fileName) {
        try {
            File dir = new File(targetDir);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IllegalStateException("Cannot create backup dir: " + targetDir);
            }
            if (fileName == null || fileName.isBlank()) {
                String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
                fileName = "h2-backup-" + ts + ".zip";
            }
            File out = new File(dir, fileName);
            String sql = "SCRIPT TO '" + out.getAbsolutePath().replace("\\", "/") + "' COMPRESSION ZIP;";
            jdbcTemplate.execute(sql);
            log.info("H2 backup created at {}", out.getAbsolutePath());
            return out;
        } catch (Exception e) {
            log.error("Backup failed", e);
            throw e;
        }
    }
}
