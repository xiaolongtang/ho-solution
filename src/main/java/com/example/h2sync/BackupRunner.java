package com.example.h2sync;

import com.example.h2sync.service.BackupService;
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

    public BackupRunner(BackupService backupService) {
        this.backupService = backupService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (args.containsOption("backup")) {
            String dir = args.getOptionValues("backup.dir") != null ? args.getOptionValues("backup.dir").get(0) : "backups";
            String file = args.getOptionValues("backup.file") != null ? args.getOptionValues("backup.file").get(0) : null;
            File out = backupService.backupTo(dir, file);
            log.info("Backup file: {}", out.getAbsolutePath());
            System.exit(0);
        }
    }
}
