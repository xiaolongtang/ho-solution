package com.example.h2sync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;
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
        return backupTo(targetDir, fileName, "h2-backup");
    }

    public File backupTo(String targetDir, String fileName, String filePrefix) {
        try {
            File dir = new File(targetDir);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IllegalStateException("Cannot create backup dir: " + targetDir);
            }
            if (fileName == null || fileName.isBlank()) {
                String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
                String prefix = filePrefix == null || filePrefix.isBlank() ? "h2-backup" : filePrefix;
                fileName = prefix + "-" + ts + ".zip";
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

    public void cleanupOldBackups(String targetDir, int retainCount) {
        if (retainCount < 1) {
            log.info("Retention count is {}. Skipping cleanup for directory {}.", retainCount, targetDir);
            return;
        }

        File dir = new File(targetDir);
        if (!dir.exists() || !dir.isDirectory()) {
            log.info("Backup directory {} does not exist; skipping cleanup.", targetDir);
            return;
        }

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".zip"));
        if (files == null || files.length <= retainCount) {
            log.debug("Found {} backup files in {} which is within retention count {}; no cleanup required.",
                    files == null ? 0 : files.length, targetDir, retainCount);
            return;
        }

        Arrays.sort(files, Comparator.comparingLong(this::getCreationTimeMillis).reversed());

        for (int i = retainCount; i < files.length; i++) {
            File file = files[i];
            if (file.delete()) {
                log.info("Deleted old backup file {}", file.getAbsolutePath());
            } else {
                log.warn("Failed to delete old backup file {}", file.getAbsolutePath());
            }
        }
    }

    private long getCreationTimeMillis(File file) {
        try {
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            return attrs.creationTime().toMillis();
        } catch (IOException e) {
            log.warn("Failed to read creation time for {}. Falling back to lastModified.", file.getAbsolutePath(), e);
            return file.lastModified();
        }
    }
}
