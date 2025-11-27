package com.example.h2sync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class H2SyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(H2SyncApplication.class, args);
    }
}
