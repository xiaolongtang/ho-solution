package com.example.h2sync.controller;

import com.example.h2sync.service.OracleSampleLoaderService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/api/sample-loader")
public class SampleLoaderController {

    private final OracleSampleLoaderService sampleLoaderService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SampleLoaderController(OracleSampleLoaderService sampleLoaderService) {
        this.sampleLoaderService = sampleLoaderService;
    }

    @Operation(
            summary = "Trigger a limited Oracle-to-H2 load",
            description = "Loads tables with up to the configured row limit (100 by default), recreates views, and syncs sequences into the sample H2 database."
    )
    @GetMapping("/refresh")
    public ResponseEntity<String> triggerSampleLoad() {
        if (!running.compareAndSet(false, true)) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Sample load already running.");
        }
        try {
            sampleLoaderService.runSampleLoad();
            String message = String.format(
                    "Sample refresh completed successfully. H2 URL: %s (row limit %d rows per table).",
                    sampleLoaderService.getSampleH2Url(),
                    sampleLoaderService.getRowLimit()
            );
            return ResponseEntity.ok(message);
        } finally {
            running.set(false);
        }
    }
}
