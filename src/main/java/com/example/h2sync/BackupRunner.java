package com.example.h2sync;

import com.example.h2sync.service.BackupService;
import com.example.h2sync.config.BackupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class BackupRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(BackupRunner.class);
    private final BackupService backupService;
    private final BackupProperties backupProperties;

    public BackupRunner(BackupService backupService, BackupProperties backupProperties) {
        this.backupService = backupService;
        this.backupProperties = backupProperties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (args.containsOption("backup")) {
            String dir = args.getOptionValues("backup.dir") != null ? args.getOptionValues("backup.dir").get(0) : backupProperties.getDir();
            String file = args.getOptionValues("backup.file") != null ? args.getOptionValues("backup.file").get(0) : null;
            File out = backupService.backupTo(dir, file, backupProperties.getFilePrefix());
            log.info("Backup file: {}", out.getAbsolutePath());
            System.exit(0);
        }
    }
}
