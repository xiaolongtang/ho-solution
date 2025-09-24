package com.example.h2sync.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

@Service
public class OracleLoaderService extends AbstractOracleLoaderService {

    @Autowired
    public OracleLoaderService(
            JdbcTemplate h2,
            @Value("${oracle.driver-class:oracle.jdbc.OracleDriver}") String driverClass,
            @Value("${oracle.url}") String url,
            @Value("${oracle.username}") String user,
            @Value("${oracle.password}") String pass,
            @Value("${oracle.schema}") String schema,
            @Value("${loader.threads:4}") int threads,
            @Value("${loader.batchSize:1000}") int batchSize,
            @Value("${loader.maxRetries:3}") int maxRetries,
            @Value("#{'${loader.blacklist:}'.replace('[','').replace(']','')}") String blacklistCsv
    ) {
        this(h2, createOracleDataSource(driverClass, url, user, pass), schema, threads, batchSize, maxRetries, blacklistCsv);
    }

    OracleLoaderService(
            JdbcTemplate h2,
            DataSource oracleDs,
            String schema,
            int threads,
            int batchSize,
            int maxRetries,
            String blacklistCsv
    ) {
        super(h2, oracleDs, schema, threads, batchSize, maxRetries, blacklistCsv);
    }

    @Override
    protected String getJobName() {
        return "Oracle -> H2 full refresh";
    }
}
