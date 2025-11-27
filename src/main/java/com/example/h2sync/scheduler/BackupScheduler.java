package com.example.h2sync.scheduler;

import com.example.h2sync.config.BackupProperties;
import com.example.h2sync.service.BackupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BackupScheduler {
    private static final Logger log = LoggerFactory.getLogger(BackupScheduler.class);

    private final BackupService backupService;
    private final BackupProperties properties;
    private final AtomicBoolean shutdownBackupTriggered = new AtomicBoolean(false);

    public BackupScheduler(BackupService backupService, BackupProperties properties) {
        this.backupService = backupService;
        this.properties = properties;
    }

    @Scheduled(cron = "${backup.cron}")
    public void scheduledBackup() {
        performBackup("scheduled cron expression");
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed(ContextClosedEvent event) {
        if (!properties.isEnabled()) {
            return;
        }
        if (shutdownBackupTriggered.compareAndSet(false, true)) {
            String reason = "application shutdown (" + event.getSource().getClass().getSimpleName() + ")";
            performBackup(reason);
        } else {
            log.debug("Shutdown backup already triggered; ignoring subsequent ContextClosedEvent.");
        }
    }

    private void performBackup(String reason) {
        if (!properties.isEnabled()) {
            log.info("Backup skipped for '{}' because backup.enabled=false", reason);
            return;
        }
        try {
            log.info("Starting H2 backup triggered by {}", reason);
            File out = backupService.backupTo(properties.getDir(), null, properties.getFilePrefix());
            log.info("Backup triggered by {} completed: {}", reason, out.getAbsolutePath());
            backupService.cleanupOldBackups(properties.getDir(), properties.getRetentionCount());
        } catch (Exception e) {
            log.error("Backup triggered by '{}' failed", reason, e);
        }
    }
}
