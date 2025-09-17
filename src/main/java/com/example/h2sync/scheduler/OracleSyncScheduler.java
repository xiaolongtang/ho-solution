package com.example.h2sync.scheduler;

import com.example.h2sync.service.OracleLoaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class OracleSyncScheduler implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(OracleSyncScheduler.class);

    private final OracleLoaderService loader;
    private final boolean enabled;

    public OracleSyncScheduler(OracleLoaderService loader,
                               @Value("${loader.enabled:true}") boolean enabled) {
        this.loader = loader;
        this.enabled = enabled;
    }

    @Scheduled(cron = "${loader.cron}")
    public void scheduled() {
        if (!enabled) return;
        loader.runFullRefresh();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (enabled && args.containsOption("runOnce")) {
            log.info("runOnce flag detected; performing a single full refresh now.");
            loader.runFullRefresh();
        }
    }
}
