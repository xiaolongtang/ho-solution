package com.example.h2sync.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "backup")
public class BackupProperties {
    private boolean enabled = true;
    private String dir = "backups";
    private String cron = "0 0 22 * * *";
    private int retentionCount = 10;
    private String filePrefix = "h2-backup";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public int getRetentionCount() {
        return retentionCount;
    }

    public void setRetentionCount(int retentionCount) {
        this.retentionCount = retentionCount;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }
}
