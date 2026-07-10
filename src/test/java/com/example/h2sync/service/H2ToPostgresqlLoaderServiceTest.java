package com.example.h2sync.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2ToPostgresqlLoaderServiceTest {

    @Test
    void fullRefreshCopiesDataConstraintsIndexesForeignKeysViewsAndSequences() throws Exception {
        DriverManagerDataSource sourceDataSource = dataSource("source", "Oracle");
        DriverManagerDataSource targetDataSource = dataSource("target", "PostgreSQL");
        JdbcTemplate source = new JdbcTemplate(sourceDataSource);
        JdbcTemplate target = new JdbcTemplate(targetDataSource);
        setupSource(source);

        H2ToPostgresqlLoaderService loader = new H2ToPostgresqlLoaderService(
                source,
                sourceDataSource,
                targetDataSource,
                "PUBLIC",
                "TARGET",
                1,
                3,
                2,
                2,
                ""
        );

        loader.runFullRefresh();

        assertEquals(2, target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"DEPARTMENT\"", Integer.class));
        assertEquals(3, target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"EMPLOYEE\"", Integer.class));
        assertEquals("Alice", target.queryForObject(
                "SELECT \"NAME\" FROM \"TARGET\".\"EMPLOYEE\" WHERE \"ID\" = 1", String.class));
        assertArrayEquals(new byte[]{1, 2}, target.queryForObject(
                "SELECT \"PHOTO\" FROM \"TARGET\".\"EMPLOYEE\" WHERE \"ID\" = 1", byte[].class));
        assertEquals("first", target.queryForObject(
                "SELECT \"NOTES\" FROM \"TARGET\".\"EMPLOYEE\" WHERE \"ID\" = 1", String.class));
        assertEquals("Engineering", target.queryForObject(
                "SELECT \"DEPARTMENT_CODE\" FROM \"TARGET\".\"EMPLOYEE_VIEW\" WHERE \"ID\" = 2", String.class));
        target.update("INSERT INTO \"TARGET\".\"AUDIT_EVENT\" (\"MESSAGE\") VALUES (?)", "third");
        assertEquals(14L, target.queryForObject(
                "SELECT MAX(\"ID\") FROM \"TARGET\".\"AUDIT_EVENT\"", Long.class));
        assertEquals("THIRD", target.queryForObject(
                "SELECT \"MESSAGE_UPPER\" FROM \"TARGET\".\"AUDIT_EVENT\" WHERE \"ID\" = 14",
                String.class));

        Set<String> constraints = new HashSet<>(target.queryForList(
                "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
                        "WHERE TABLE_SCHEMA = 'TARGET' AND TABLE_NAME = 'EMPLOYEE'",
                String.class
        ));
        assertTrue(constraints.contains("PK_EMPLOYEE"));
        assertTrue(constraints.contains("UQ_EMPLOYEE_EMAIL"));
        assertTrue(constraints.contains("FK_EMPLOYEE_DEPARTMENT"));

        Set<String> indexes = indexNames(targetDataSource, "TARGET", "EMPLOYEE");
        assertTrue(indexes.contains("IDX_EMPLOYEE_NAME"));
        assertTrue(indexes.contains("UX_EMPLOYEE_NAME_DEPT"));

        assertEquals(new BigDecimal("100"), target.queryForObject(
                "SELECT BASE_VALUE FROM INFORMATION_SCHEMA.SEQUENCES " +
                        "WHERE SEQUENCE_SCHEMA = 'TARGET' AND SEQUENCE_NAME = 'EMPLOYEE_SEQ'",
                BigDecimal.class
        ));

        assertEquals(0, target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"H2_PG_ETL_FAIL_LOG\" WHERE \"ATTEMPT_COUNT\" > 0",
                Integer.class
        ));
        assertTrue(target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"H2_PG_ETL_FAIL_LOG\"", Integer.class) >= 7);
    }

    @Test
    void fullRefreshHonorsCaseInsensitiveBlacklist() {
        DriverManagerDataSource sourceDataSource = dataSource("blacklist-source", "Oracle");
        DriverManagerDataSource targetDataSource = dataSource("blacklist-target", "PostgreSQL");
        JdbcTemplate source = new JdbcTemplate(sourceDataSource);
        setupSource(source);

        H2ToPostgresqlLoaderService loader = new H2ToPostgresqlLoaderService(
                source,
                sourceDataSource,
                targetDataSource,
                "PUBLIC",
                "TARGET",
                1,
                2,
                10,
                1,
                "public.employee, employee_seq, employee_view, employee_view_2"
        );
        loader.runFullRefresh();

        JdbcTemplate target = new JdbcTemplate(targetDataSource);
        assertEquals(0, target.queryForObject(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                        "WHERE TABLE_SCHEMA = 'TARGET' AND TABLE_NAME = 'EMPLOYEE'",
                Integer.class
        ));
        assertEquals(0, target.queryForObject(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SEQUENCES " +
                        "WHERE SEQUENCE_SCHEMA = 'TARGET' AND SEQUENCE_NAME = 'EMPLOYEE_SEQ'",
                Integer.class
        ));
        assertEquals(2, target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"DEPARTMENT\"", Integer.class));
    }

    @Test
    void terminalObjectFailureMakesTheWholeMigrationFail() {
        DriverManagerDataSource sourceDataSource = dataSource("failure-source", "Oracle");
        DriverManagerDataSource targetDataSource = dataSource("failure-target", "PostgreSQL");
        JdbcTemplate source = new JdbcTemplate(sourceDataSource);
        setupSource(source);

        H2ToPostgresqlLoaderService loader = new H2ToPostgresqlLoaderService(
                source,
                sourceDataSource,
                targetDataSource,
                "PUBLIC",
                "TARGET",
                1,
                2,
                10,
                1,
                "employee"
        );

        H2ToPostgresqlLoaderService.MigrationIncompleteException error = assertThrows(
                H2ToPostgresqlLoaderService.MigrationIncompleteException.class,
                loader::runFullRefresh
        );

        assertTrue(error.getFailureCount() >= 1);
        JdbcTemplate target = new JdbcTemplate(targetDataSource);
        assertTrue(target.queryForObject(
                "SELECT COUNT(*) FROM \"TARGET\".\"H2_PG_ETL_FAIL_LOG\" WHERE \"ATTEMPT_COUNT\" > 0",
                Integer.class
        ) >= 1);
    }

    @Test
    void viewTranslatorChangesOnlyDialectTokensOutsideQuotedContent() {
        H2ToPostgresqlViewSqlTranslator translator =
                new H2ToPostgresqlViewSqlTranslator("PUBLIC", "reporting");

        String translated = translator.translate(
                "SELECT NVL(\"NAME\", 'NVL(SYSDATE)'), SYSDATE FROM \"PUBLIC\".\"EMPLOYEE\";"
        );

        assertEquals(
                "SELECT COALESCE(\"NAME\", 'NVL(SYSDATE)'), CURRENT_TIMESTAMP " +
                        "FROM \"reporting\".\"EMPLOYEE\"",
                translated
        );
    }

    @Test
    void viewTranslatorRewritesCanonicalH2DateAndAggregateFunctions() {
        H2ToPostgresqlViewSqlTranslator translator =
                new H2ToPostgresqlViewSqlTranslator("PUBLIC", "reporting");

        String translated = translator.translate(
                "SELECT DATE_TRUNC(DAY, \"D\"), ADD_MONTHS(\"D\", 2), LAST_DAY(\"D\"), " +
                        "LISTAGG(\"NAME\", ',') WITHIN GROUP (ORDER BY \"ID\") " +
                        "FROM \"PUBLIC\".\"EMP\""
        );

        assertEquals(
                "SELECT DATE_TRUNC('day', \"D\"), (\"D\" + (2) * INTERVAL '1 month'), " +
                        "(DATE_TRUNC('month', \"D\") + INTERVAL '1 month - 1 day')::date, " +
                        "STRING_AGG(\"NAME\", ',' ORDER BY \"ID\") FROM \"reporting\".\"EMP\"",
                translated
        );
    }

    private static void setupSource(JdbcTemplate jdbc) {
        jdbc.execute("CREATE TABLE \"DEPARTMENT\" (" +
                "\"ID\" INTEGER NOT NULL, " +
                "\"CODE\" VARCHAR(32) NOT NULL, " +
                "CONSTRAINT \"PK_DEPARTMENT\" PRIMARY KEY (\"ID\"), " +
                "CONSTRAINT \"UQ_DEPARTMENT_CODE\" UNIQUE (\"CODE\"))");
        jdbc.execute("CREATE TABLE \"EMPLOYEE\" (" +
                "\"ID\" BIGINT NOT NULL, " +
                "\"DEPARTMENT_ID\" INTEGER NOT NULL, " +
                "\"NAME\" VARCHAR(64) NOT NULL, " +
                "\"EMAIL\" VARCHAR(128), " +
                "\"SALARY\" DECIMAL(12,2), " +
                "\"PHOTO\" BLOB, " +
                "\"NOTES\" CLOB, " +
                "CONSTRAINT \"PK_EMPLOYEE\" PRIMARY KEY (\"ID\"), " +
                "CONSTRAINT \"UQ_EMPLOYEE_EMAIL\" UNIQUE (\"EMAIL\"), " +
                "CONSTRAINT \"FK_EMPLOYEE_DEPARTMENT\" FOREIGN KEY (\"DEPARTMENT_ID\") " +
                "REFERENCES \"DEPARTMENT\" (\"ID\"))");
        jdbc.execute("CREATE TABLE \"AUDIT_EVENT\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 2), " +
                "\"MESSAGE\" VARCHAR(128) NOT NULL, " +
                "\"MESSAGE_UPPER\" VARCHAR(128) GENERATED ALWAYS AS (UPPER(\"MESSAGE\")), " +
                "CONSTRAINT \"PK_AUDIT_EVENT\" PRIMARY KEY (\"ID\"))");
        jdbc.execute("CREATE INDEX \"IDX_EMPLOYEE_NAME\" ON \"EMPLOYEE\" (\"NAME\")");
        jdbc.execute("CREATE UNIQUE INDEX \"UX_EMPLOYEE_NAME_DEPT\" " +
                "ON \"EMPLOYEE\" (\"NAME\", \"DEPARTMENT_ID\")");
        jdbc.execute("CREATE SEQUENCE \"EMPLOYEE_SEQ\" START WITH 100 INCREMENT BY 5");

        jdbc.update("INSERT INTO \"DEPARTMENT\" (\"ID\", \"CODE\") VALUES (?, ?)", 10, "Sales");
        jdbc.update("INSERT INTO \"DEPARTMENT\" (\"ID\", \"CODE\") VALUES (?, ?)", 20, "Engineering");
        jdbc.update("INSERT INTO \"EMPLOYEE\" VALUES (?, ?, ?, ?, ?, ?, ?)",
                1L, 10, "Alice", "alice@example.com", new BigDecimal("100.10"),
                new byte[]{1, 2}, "first");
        jdbc.update("INSERT INTO \"EMPLOYEE\" VALUES (?, ?, ?, ?, ?, ?, ?)",
                2L, 20, "Bob", "bob@example.com", new BigDecimal("200.20"),
                new byte[]{3, 4}, "second");
        jdbc.update("INSERT INTO \"EMPLOYEE\" VALUES (?, ?, ?, ?, ?, ?, ?)",
                3L, 20, "Carol", null, new BigDecimal("300.30"), null, null);
        jdbc.update("INSERT INTO \"AUDIT_EVENT\" (\"MESSAGE\") VALUES (?)", "first");
        jdbc.update("INSERT INTO \"AUDIT_EVENT\" (\"MESSAGE\") VALUES (?)", "second");

        jdbc.execute("CREATE VIEW \"EMPLOYEE_VIEW\" (\"ID\", \"NAME\", \"DEPARTMENT_CODE\") AS " +
                "SELECT E.\"ID\", E.\"NAME\", D.\"CODE\" " +
                "FROM \"EMPLOYEE\" E JOIN \"DEPARTMENT\" D ON E.\"DEPARTMENT_ID\" = D.\"ID\"");
        jdbc.execute("CREATE VIEW \"EMPLOYEE_VIEW_2\" AS " +
                "SELECT \"ID\", \"NAME\" FROM \"EMPLOYEE_VIEW\"");
    }

    private static DriverManagerDataSource dataSource(String prefix, String mode) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:mem:" + prefix + randomSuffix() +
                ";MODE=" + mode + ";DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase(Locale.ROOT);
    }

    private static Set<String> indexNames(DriverManagerDataSource dataSource, String schema, String table)
            throws SQLException {
        try (Connection connection = dataSource.getConnection();
             ResultSet rs = connection.getMetaData().getIndexInfo(connection.getCatalog(), schema, table, false, false)) {
            Set<String> result = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("INDEX_NAME");
                if (name != null) result.add(name);
            }
            return result;
        }
    }

}
