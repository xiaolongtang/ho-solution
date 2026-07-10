package com.example.h2sync.controller;

import com.example.h2sync.scheduler.H2PostgresqlSyncScheduler;
import com.example.h2sync.scheduler.H2PostgresqlSyncScheduler.TriggerResult;
import com.example.h2sync.service.H2ToPostgresqlLoaderService.MigrationIncompleteException;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/postgresql-loader")
public class H2PostgresqlLoaderController {

    private final H2PostgresqlSyncScheduler scheduler;

    public H2PostgresqlLoaderController(H2PostgresqlSyncScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Operation(
            summary = "Trigger a full H2-to-PostgreSQL load",
            description = "Rebuilds tables and data, sequences, indexes, foreign keys and views, then prints a validation report."
    )
    @PostMapping("/full-refresh")
    public ResponseEntity<String> triggerFullRefresh(
            @RequestParam(value = "reason", required = false) String reason
    ) {
        String triggerReason = reason == null || reason.isBlank()
                ? "manual API request"
                : "manual API request: " + reason;
        TriggerResult result;
        try {
            result = scheduler.triggerFullRefresh(triggerReason);
        } catch (MigrationIncompleteException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("H2 -> PostgreSQL refresh finished with " + ex.getFailureCount() +
                            " failed objects. Check H2_PG_ETL_FAIL_LOG and the migration report.");
        }
        return switch (result) {
            case STARTED -> ResponseEntity.ok("H2 -> PostgreSQL full refresh completed successfully.");
            case ALREADY_RUNNING -> ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("H2 -> PostgreSQL refresh skipped because another run is still in progress.");
            case DISABLED -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("H2 -> PostgreSQL refresh skipped because postgresql.loader.enabled=false.");
        };
    }
}
