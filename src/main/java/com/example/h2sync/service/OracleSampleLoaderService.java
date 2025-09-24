package com.example.h2sync.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

@Service
public class OracleSampleLoaderService extends AbstractOracleLoaderService {

    private final int rowLimit;
    private final String h2Url;

    @Autowired
    public OracleSampleLoaderService(
            @Value("${sample.loader.h2-url:jdbc:h2:./data-sample/h2db;MODE=Oracle;DATABASE_TO_UPPER=false;AUTO_SERVER=TRUE}") String h2Url,
            @Value("${sample.loader.h2-username:sa}") String h2Username,
            @Value("${sample.loader.h2-password:}") String h2Password,
            @Value("${oracle.driver-class:oracle.jdbc.OracleDriver}") String driverClass,
            @Value("${oracle.url}") String url,
            @Value("${oracle.username}") String user,
            @Value("${oracle.password}") String pass,
            @Value("${oracle.schema}") String schema,
            @Value("${sample.loader.threads:${loader.threads:4}}") int threads,
            @Value("${sample.loader.batchSize:${loader.batchSize:1000}}") int batchSize,
            @Value("${sample.loader.maxRetries:${loader.maxRetries:3}}") int maxRetries,
            @Value("#{'${sample.loader.blacklist:${loader.blacklist:}}'.replace('[','').replace(']','')}") String blacklistCsv,
            @Value("${sample.loader.row-limit:100}") int rowLimit
    ) {
        this(createSampleJdbcTemplate(h2Url, h2Username, h2Password),
                createOracleDataSource(driverClass, url, user, pass),
                schema, threads, batchSize, maxRetries, blacklistCsv, rowLimit, h2Url);
    }

    OracleSampleLoaderService(
            JdbcTemplate h2,
            DataSource oracleDs,
            String schema,
            int threads,
            int batchSize,
            int maxRetries,
            String blacklistCsv,
            int rowLimit,
            String h2Url
    ) {
        super(h2, oracleDs, schema, threads, batchSize, maxRetries, blacklistCsv);
        this.rowLimit = rowLimit > 0 ? rowLimit : 100;
        this.h2Url = h2Url;
    }

    private static JdbcTemplate createSampleJdbcTemplate(String url, String user, String pass) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        return new JdbcTemplate(ds);
    }

    @Override
    protected String getJobName() {
        return "Oracle -> H2 sample refresh";
    }

    @Override
    protected String buildTableSelectSql(String oracleQualifiedTable, String table) {
        return "SELECT * FROM " + oracleQualifiedTable + " FETCH FIRST " + rowLimit + " ROWS ONLY";
    }

    public void runSampleLoad() {
        runFullRefresh();
    }

    public String getSampleH2Url() {
        return h2Url;
    }

    public int getRowLimit() {
        return rowLimit;
    }
}
