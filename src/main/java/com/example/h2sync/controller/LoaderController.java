package com.example.h2sync.controller;

import com.example.h2sync.scheduler.OracleSyncScheduler;
import com.example.h2sync.scheduler.OracleSyncScheduler.TriggerResult;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/loader")
public class LoaderController {

    private final OracleSyncScheduler scheduler;

    public LoaderController(OracleSyncScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Operation(
            summary = "Trigger a full Oracle-to-H2 load",
            description = "Runs the same full refresh used by the scheduler. The call blocks until the refresh completes."
    )
    @PostMapping(path = "/full-refresh")
    public ResponseEntity<String> triggerFullRefresh(
            @RequestParam(value = "reason", required = false) String reason
    ) {
        String triggerReason = (reason == null || reason.isBlank())
                ? "manual API request"
                : "manual API request: " + reason;

        TriggerResult result = scheduler.triggerFullRefresh(triggerReason);
        return switch (result) {
            case STARTED -> ResponseEntity.ok("Full refresh completed successfully.");
            case ALREADY_RUNNING -> ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Full refresh skipped because another run is still in progress.");
            case DISABLED -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("Full refresh skipped because loader.enabled=false.");
        };
    }
}
