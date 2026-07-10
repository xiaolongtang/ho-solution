package com.example.h2sync.scheduler;

import com.example.h2sync.service.H2ToPostgresqlLoaderService;
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
public class H2PostgresqlSyncScheduler implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(H2PostgresqlSyncScheduler.class);

    private final H2ToPostgresqlLoaderService loader;
    private final boolean enabled;
    private final boolean startupEnabled;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean startupTriggered = new AtomicBoolean(false);
    private volatile String startupReason = "application startup";

    public enum TriggerResult {
        STARTED,
        ALREADY_RUNNING,
        DISABLED
    }

    public H2PostgresqlSyncScheduler(
            H2ToPostgresqlLoaderService loader,
            @Value("${postgresql.loader.enabled:false}") boolean enabled,
            @Value("${postgresql.loader.startup:true}") boolean startupEnabled
    ) {
        this.loader = loader;
        this.enabled = enabled;
        this.startupEnabled = startupEnabled;
    }

    @Scheduled(
            cron = "${postgresql.loader.cron:0 30 3 * * *}",
            zone = "${postgresql.loader.zone:Asia/Shanghai}"
    )
    public void scheduled() {
        triggerFullRefresh("scheduled cron expression");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!startupEnabled) {
            log.info("H2 -> PostgreSQL startup refresh skipped because postgresql.loader.startup=false");
            return;
        }
        triggerStartupRefresh(startupReason);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.containsOption("postgresqlRunOnce")) {
            startupReason = "postgresqlRunOnce command-line flag";
            log.info("postgresqlRunOnce flag detected; H2 -> PostgreSQL refresh will run when the application is ready.");
        }
    }

    public TriggerResult triggerFullRefresh(String reason) {
        if (!enabled) {
            log.info("H2 -> PostgreSQL trigger '{}' skipped because postgresql.loader.enabled=false", reason);
            return TriggerResult.DISABLED;
        }
        if (!running.compareAndSet(false, true)) {
            log.info("H2 -> PostgreSQL trigger '{}' skipped because another refresh is running", reason);
            return TriggerResult.ALREADY_RUNNING;
        }
        try {
            log.info("Starting H2 -> PostgreSQL full refresh (triggered by {}).", reason);
            loader.runFullRefresh();
            log.info("H2 -> PostgreSQL refresh triggered by '{}' finished successfully.", reason);
            return TriggerResult.STARTED;
        } finally {
            running.set(false);
        }
    }

    private void triggerStartupRefresh(String reason) {
        if (!startupTriggered.compareAndSet(false, true)) {
            log.debug("H2 -> PostgreSQL startup refresh already triggered; skipping '{}'.", reason);
            return;
        }
        triggerFullRefresh(reason);
    }
}
