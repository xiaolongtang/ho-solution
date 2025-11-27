package com.example.h2sync.scheduler;

import com.example.h2sync.service.OracleLoaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class OracleSyncScheduler implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(OracleSyncScheduler.class);

    private final OracleLoaderService loader;
    private final boolean enabled;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean startupTriggered = new AtomicBoolean(false);
    private volatile String startupReason = "application startup";

    public enum TriggerResult {
        STARTED,
        ALREADY_RUNNING,
        DISABLED
    }

    public OracleSyncScheduler(OracleLoaderService loader,
                               @Value("${loader.enabled:true}") boolean enabled) {
        this.loader = loader;
        this.enabled = enabled;
    }

    @Scheduled(cron = "${loader.cron}")
    public void scheduled() {
        triggerFullRefresh("scheduled cron expression");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        triggerStartupRefresh(startupReason);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (args.containsOption("runOnce")) {
            startupReason = "runOnce command-line flag";
            log.info("runOnce flag detected; scheduling startup full refresh once the application is ready.");
        }
    }

    public TriggerResult triggerFullRefresh(String reason) {
        if (!enabled) {
            log.info("Full refresh trigger '{}' skipped because loader.enabled=false", reason);
            return TriggerResult.DISABLED;
        }
        if (!running.compareAndSet(false, true)) {
            log.info("Full refresh trigger '{}' skipped because another refresh is already running", reason);
            return TriggerResult.ALREADY_RUNNING;
        }
        try {
            log.info("Starting full refresh (triggered by {}).", reason);
            loader.runFullRefresh();
            log.info("Full refresh triggered by '{}' finished successfully.", reason);
            return TriggerResult.STARTED;
        } finally {
            running.set(false);
        }
    }

    private void triggerStartupRefresh(String reason) {
        if (!startupTriggered.compareAndSet(false, true)) {
            log.debug("Startup full refresh already triggered; skipping '{}'.", reason);
            return;
        }
        TriggerResult result = triggerFullRefresh(reason);
        if (result == TriggerResult.ALREADY_RUNNING) {
            log.info("Startup full refresh '{}' is already handled by another trigger.", reason);
        }
    }
}
