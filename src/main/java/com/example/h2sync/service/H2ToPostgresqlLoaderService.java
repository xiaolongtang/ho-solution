package com.example.h2sync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Full-refresh migration from the application's H2 database to PostgreSQL.
 * Tables are rebuilt and streamed first, followed by indexes, foreign keys and
 * dependency-ordered views. Explicit sequences are recreated at their H2 next
 * value before table DDL is applied so sequence-backed defaults remain valid.
 */
@Service
public class H2ToPostgresqlLoaderService {

    private static final Logger log = LoggerFactory.getLogger(H2ToPostgresqlLoaderService.class);
    private static final String FAILURE_TABLE = "H2_PG_ETL_FAIL_LOG";
    private static final int POSTGRESQL_MAX_VARCHAR_LENGTH = 10_485_760;
    private static final Pattern NEXT_VALUE_PATTERN = Pattern.compile(
            "(?i)^NEXT\\s+VALUE\\s+FOR\\s+(?:(?:\\\"([^\\\"]+)\\\"|([A-Z0-9_$#]+))\\s*\\.\\s*)?(?:\\\"([^\\\"]+)\\\"|([A-Z0-9_$#]+))$"
    );

    private final JdbcTemplate source;
    private final DataSource sourceDataSource;
    private final JdbcTemplate target;
    private final DataSource targetDataSource;
    private final String sourceSchema;
    private final String targetSchema;
    private final int sourceReadThreads;
    private final int targetThreads;
    private final int batchSize;
    private final int maxRetries;
    private final Set<String> blacklist;
    private final H2ToPostgresqlViewSqlTranslator viewSqlTranslator;
    private final H2ToPostgresqlMigrationReportPrinter reportPrinter;
    private final boolean h2TestTarget;

    public H2ToPostgresqlLoaderService(
            JdbcTemplate source,
            @Value("${postgresql.driver-class:org.postgresql.Driver}") String driverClass,
            @Value("${postgresql.url:jdbc:postgresql://localhost:5432/YOUR_DATABASE}") String url,
            @Value("${postgresql.username:YOUR_POSTGRESQL_USER}") String user,
            @Value("${postgresql.password:YOUR_POSTGRESQL_PASSWORD}") String pass,
            @Value("${postgresql.schema:public}") String targetSchema,
            @Value("${postgresql.loader.source-schema:PUBLIC}") String sourceSchema,
            @Value("${postgresql.loader.source-read-threads:1}") int sourceReadThreads,
            @Value("${postgresql.loader.target-threads:4}") int targetThreads,
            @Value("${postgresql.loader.batchSize:${loader.batchSize:1000}}") int batchSize,
            @Value("${postgresql.loader.maxRetries:${loader.maxRetries:3}}") int maxRetries,
            @Value("#{'${postgresql.loader.blacklist:}'.replace('[','').replace(']','')}") String blacklistCsv
    ) {
        this(
                source,
                Objects.requireNonNull(source.getDataSource(), "H2 source DataSource is required"),
                createPostgresqlDataSource(driverClass, url, user, pass),
                sourceSchema,
                targetSchema,
                sourceReadThreads,
                targetThreads,
                batchSize,
                maxRetries,
                blacklistCsv
        );
    }

    H2ToPostgresqlLoaderService(
            JdbcTemplate source,
            DataSource sourceDataSource,
            DataSource targetDataSource,
            String sourceSchema,
            String targetSchema,
            int sourceReadThreads,
            int targetThreads,
            int batchSize,
            int maxRetries,
            String blacklistCsv
    ) {
        this.source = Objects.requireNonNull(source, "source");
        this.sourceDataSource = Objects.requireNonNull(sourceDataSource, "sourceDataSource");
        this.targetDataSource = Objects.requireNonNull(targetDataSource, "targetDataSource");
        this.target = new JdbcTemplate(targetDataSource);
        this.sourceSchema = requireIdentifier(sourceSchema, "sourceSchema");
        this.targetSchema = requireIdentifier(targetSchema, "targetSchema");
        this.sourceReadThreads = Math.max(1, sourceReadThreads);
        this.targetThreads = Math.max(1, targetThreads);
        this.batchSize = Math.max(1, batchSize);
        this.maxRetries = Math.max(1, maxRetries);
        this.blacklist = parseBlacklist(blacklistCsv);
        this.viewSqlTranslator = new H2ToPostgresqlViewSqlTranslator(this.sourceSchema, this.targetSchema);
        this.h2TestTarget = detectH2Target(targetDataSource);
        this.reportPrinter = new H2ToPostgresqlMigrationReportPrinter(
                log,
                source,
                target,
                targetDataSource,
                this.sourceSchema,
                this.targetSchema,
                this::isBlacklisted,
                this.h2TestTarget
        );
    }

    private static DataSource createPostgresqlDataSource(String driverClass, String url, String user, String pass) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName(driverClass == null || driverClass.isBlank()
                ? "org.postgresql.Driver"
                : driverClass);
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        if (url != null && url.toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql:")) {
            Properties properties = new Properties();
            properties.setProperty("reWriteBatchedInserts", "true");
            ds.setConnectionProperties(properties);
        }
        return ds;
    }

    public void runFullRefresh() {
        log.info("Starting H2 -> PostgreSQL full refresh. sourceReadThreads={}, targetThreads={}, " +
                        "batchSize={}, sourceSchema={}, targetSchema={}, blacklist={}",
                sourceReadThreads, targetThreads, batchSize, sourceSchema, targetSchema, blacklist);
        if (sourceReadThreads > 2) {
            log.warn("postgresql.loader.source-read-threads={} may put excessive I/O pressure on embedded H2; " +
                    "1 is recommended unless file-based benchmarks show a benefit", sourceReadThreads);
        }
        long started = System.currentTimeMillis();

        MigrationSnapshot snapshot = readSourceSnapshot();
        initializeTarget();
        dropSourceViewsFromTarget(snapshot.views().keySet());
        List<MigrationFailure> failures = new CopyOnWriteArrayList<>();
        Set<String> successfulTables = ConcurrentHashMap.newKeySet();

        for (SequenceDefinition sequence : snapshot.sequences()) {
            if (!isBlacklisted(sequence.name())) {
                retry(() -> syncSequence(sequence), "SEQUENCE", sequence.name(), failures);
            }
        }

        List<Future<?>> futures = new ArrayList<>();
        ExecutorService sourcePool = Executors.newFixedThreadPool(sourceReadThreads);
        try {
            for (TableDefinition table : snapshot.tables().values()) {
                futures.add(sourcePool.submit(() -> {
                    if (retry(() -> copyTable(table), "TABLE", table.name(), failures)) {
                        successfulTables.add(table.name());
                    }
                }));
            }
            waitForFutures(futures, "table", failures);
        } finally {
            sourcePool.shutdown();
        }

        ExecutorService targetPool = Executors.newFixedThreadPool(targetThreads);
        try {
            for (TableDefinition table : snapshot.tables().values()) {
                if (!successfulTables.contains(table.name())) continue;
                futures.add(targetPool.submit(() -> {
                    if (table.primaryKey() != null) {
                        ConstraintDefinition primaryKey = table.primaryKey();
                        retry(
                                () -> syncPrimaryKey(table, primaryKey),
                                "PRIMARY_KEY",
                                table.name() + "." + primaryKey.name(),
                                failures
                        );
                    }
                    for (ConstraintDefinition unique : table.uniqueConstraints()) {
                        retry(
                                () -> syncUniqueConstraint(table, unique),
                                "UNIQUE",
                                table.name() + "." + unique.name(),
                                failures
                        );
                    }
                }));
            }
            waitForFutures(futures, "constraint", failures);

            for (TableDefinition table : snapshot.tables().values()) {
                if (!successfulTables.contains(table.name())) continue;
                for (IndexDefinition index : table.indexes()) {
                    String objectName = table.name() + "." + index.name();
                    futures.add(targetPool.submit(() -> retry(
                            () -> syncIndex(table, index), "INDEX", objectName, failures)));
                }
            }
            waitForFutures(futures, "index", failures);
        } finally {
            targetPool.shutdown();
        }

        for (TableDefinition table : snapshot.tables().values()) {
            if (!successfulTables.contains(table.name())) continue;
            for (ForeignKeyDefinition foreignKey : table.foreignKeys()) {
                String objectName = table.name() + "." + foreignKey.name();
                if (!isSourceSchema(foreignKey.referencedSchema())
                        || isBlacklisted(foreignKey.referencedTable())
                        || !successfulTables.contains(foreignKey.referencedTable())) {
                    log.warn("Skipping foreign key {} because referenced table {}.{} is not in the successful migration set",
                            objectName, foreignKey.referencedSchema(), foreignKey.referencedTable());
                    continue;
                }
                retry(() -> syncForeignKey(table, foreignKey), "FOREIGN_KEY", objectName, failures);
            }
        }

        syncViewsWithDependencyAwareness(snapshot.views(), failures);

        try {
            reportPrinter.printReport(snapshot);
        } catch (Exception ex) {
            log.warn("Failed to generate H2 -> PostgreSQL migration report: {}", ex.toString());
            log.debug("H2 -> PostgreSQL report failure", ex);
        }
        long took = System.currentTimeMillis() - started;
        if (!failures.isEmpty()) {
            log.error("H2 -> PostgreSQL full refresh completed with {} failed objects in {} ms", failures.size(), took);
            throw new MigrationIncompleteException(failures);
        }
        log.info("H2 -> PostgreSQL full refresh completed in {} ms", took);
    }

    private MigrationSnapshot readSourceSnapshot() {
        try (Connection connection = sourceDataSource.getConnection()) {
            Set<String> tableNames = listTables(connection);
            Set<String> viewNames = listViews(connection);
            List<SequenceDefinition> sequences = listSequences(connection);

            Map<String, TableDefinition> tables = new LinkedHashMap<>();
            for (String tableName : tableNames) {
                if (!isBlacklisted(tableName)) {
                    tables.put(tableName, loadTableDefinition(connection, tableName));
                }
            }

            Map<String, ViewDefinition> views = new LinkedHashMap<>();
            for (String viewName : viewNames) {
                if (!isBlacklisted(viewName)) {
                    views.put(viewName, loadViewDefinition(connection, viewName));
                }
            }
            return new MigrationSnapshot(tableNames, viewNames, tables, views, sequences);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to inspect H2 schema " + sourceSchema, ex);
        }
    }

    private Set<String> listTables(Connection connection) throws SQLException {
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            try (ResultSet rs = ps.executeQuery()) {
                Set<String> result = new TreeSet<>();
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                log.info("Found {} tables in H2 schema {}", result.size(), sourceSchema);
                return result;
            }
        }
    }

    private Set<String> listViews(Connection connection) throws SQLException {
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            try (ResultSet rs = ps.executeQuery()) {
                Set<String> result = new TreeSet<>();
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                log.info("Found {} views in H2 schema {}", result.size(), sourceSchema);
                return result;
            }
        }
    }

    private List<SequenceDefinition> listSequences(Connection connection) throws SQLException {
        String sql = "SELECT SEQUENCE_NAME, INCREMENT, BASE_VALUE, MINIMUM_VALUE, MAXIMUM_VALUE, " +
                "CYCLE_OPTION, CACHE FROM INFORMATION_SCHEMA.SEQUENCES " +
                "WHERE SEQUENCE_SCHEMA = ? ORDER BY SEQUENCE_NAME";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            try (ResultSet rs = ps.executeQuery()) {
                List<SequenceDefinition> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new SequenceDefinition(
                            rs.getString("SEQUENCE_NAME"),
                            rs.getBigDecimal("INCREMENT"),
                            rs.getBigDecimal("BASE_VALUE"),
                            rs.getBigDecimal("MINIMUM_VALUE"),
                            rs.getBigDecimal("MAXIMUM_VALUE"),
                            "YES".equalsIgnoreCase(rs.getString("CYCLE_OPTION")),
                            rs.getLong("CACHE")
                    ));
                }
                log.info("Found {} sequences in H2 schema {}", result.size(), sourceSchema);
                return result;
            }
        }
    }

    private TableDefinition loadTableDefinition(Connection connection, String table) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        Map<String, ColumnExtras> columnExtras = fetchColumnExtras(connection, table);
        List<ColumnDefinition> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(connection.getCatalog(), sourceSchema, table, null)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                ColumnExtras extras = columnExtras.getOrDefault(name, ColumnExtras.EMPTY);
                columns.add(new ColumnDefinition(
                        name,
                        rs.getInt("DATA_TYPE"),
                        rs.getString("TYPE_NAME"),
                        rs.getInt("COLUMN_SIZE"),
                        rs.getInt("DECIMAL_DIGITS"),
                        rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                        "YES".equalsIgnoreCase(safeGetString(rs, "IS_AUTOINCREMENT")),
                        "YES".equalsIgnoreCase(safeGetString(rs, "IS_GENERATEDCOLUMN")),
                        rs.getString("COLUMN_DEF"),
                        extras.generationExpression(),
                        extras.identityStart(),
                        extras.identityIncrement(),
                        extras.identityMinimum(),
                        extras.identityMaximum(),
                        extras.identityBase(),
                        extras.identityCycle(),
                        extras.identityCache()
                ));
            }
        }
        ConstraintDefinition primaryKey = fetchPrimaryKey(metadata, connection, table);
        List<ConstraintDefinition> uniqueConstraints = fetchUniqueConstraints(connection, table);
        List<IndexDefinition> indexes = fetchIndexes(metadata, connection, table, primaryKey, uniqueConstraints);
        List<ForeignKeyDefinition> foreignKeys = fetchForeignKeys(metadata, connection, table);
        return new TableDefinition(table, columns, primaryKey, uniqueConstraints, indexes, foreignKeys);
    }

    private Map<String, ColumnExtras> fetchColumnExtras(Connection connection, String table) throws SQLException {
        String sql = "SELECT COLUMN_NAME, GENERATION_EXPRESSION, IDENTITY_START, IDENTITY_INCREMENT, " +
                "IDENTITY_MINIMUM, IDENTITY_MAXIMUM, IDENTITY_BASE, IDENTITY_CYCLE, IDENTITY_CACHE " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, ColumnExtras> result = new HashMap<>();
                while (rs.next()) {
                    result.put(rs.getString("COLUMN_NAME"), new ColumnExtras(
                            rs.getString("GENERATION_EXPRESSION"),
                            rs.getBigDecimal("IDENTITY_START"),
                            rs.getBigDecimal("IDENTITY_INCREMENT"),
                            rs.getBigDecimal("IDENTITY_MINIMUM"),
                            rs.getBigDecimal("IDENTITY_MAXIMUM"),
                            rs.getBigDecimal("IDENTITY_BASE"),
                            "YES".equalsIgnoreCase(rs.getString("IDENTITY_CYCLE")),
                            rs.getLong("IDENTITY_CACHE")
                    ));
                }
                return result;
            }
        }
    }

    private ConstraintDefinition fetchPrimaryKey(DatabaseMetaData metadata, Connection connection, String table)
            throws SQLException {
        Map<Integer, String> columns = new LinkedHashMap<>();
        String name = null;
        try (ResultSet rs = metadata.getPrimaryKeys(connection.getCatalog(), sourceSchema, table)) {
            while (rs.next()) {
                name = rs.getString("PK_NAME");
                columns.put(rs.getInt("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
        }
        if (columns.isEmpty()) {
            return null;
        }
        String resolvedName = name == null || name.isBlank() ? "PK_" + table : name;
        return new ConstraintDefinition(resolvedName, orderedValues(columns));
    }

    private List<ConstraintDefinition> fetchUniqueConstraints(Connection connection, String table) throws SQLException {
        String sql = "SELECT TC.CONSTRAINT_NAME, KCU.COLUMN_NAME, KCU.ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU " +
                "ON TC.CONSTRAINT_CATALOG = KCU.CONSTRAINT_CATALOG " +
                "AND TC.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA " +
                "AND TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME " +
                "WHERE TC.TABLE_SCHEMA = ? AND TC.TABLE_NAME = ? AND TC.CONSTRAINT_TYPE = 'UNIQUE' " +
                "ORDER BY TC.CONSTRAINT_NAME, KCU.ORDINAL_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, List<String>> constraints = new LinkedHashMap<>();
                while (rs.next()) {
                    constraints.computeIfAbsent(rs.getString(1), ignored -> new ArrayList<>()).add(rs.getString(2));
                }
                return constraints.entrySet().stream()
                        .map(entry -> new ConstraintDefinition(entry.getKey(), entry.getValue()))
                        .toList();
            }
        }
    }

    private List<IndexDefinition> fetchIndexes(
            DatabaseMetaData metadata,
            Connection connection,
            String table,
            ConstraintDefinition primaryKey,
            List<ConstraintDefinition> uniqueConstraints
    ) throws SQLException {
        Map<String, MutableIndex> indexes = fetchIndexesFromInformationSchema(connection, table);
        if (indexes.isEmpty()) {
            indexes = fetchIndexesFromJdbcMetadata(metadata, connection, table);
        }

        List<List<String>> constraintColumns = new ArrayList<>();
        if (primaryKey != null) {
            constraintColumns.add(primaryKey.columns());
        }
        uniqueConstraints.forEach(constraint -> constraintColumns.add(constraint.columns()));

        List<IndexDefinition> result = new ArrayList<>();
        for (MutableIndex index : indexes.values()) {
            if (index.generated) {
                continue;
            }
            List<IndexColumn> indexColumns = orderedValues(index.columns);
            List<String> names = indexColumns.stream().map(IndexColumn::name).toList();
            if (index.unique && constraintColumns.stream().anyMatch(names::equals)) {
                continue;
            }
            result.add(new IndexDefinition(index.name, index.unique, indexColumns, index.filterCondition));
        }
        result.sort(Comparator.comparing(IndexDefinition::name));
        return result;
    }

    private Map<String, MutableIndex> fetchIndexesFromInformationSchema(Connection connection, String table)
            throws SQLException {
        String sql = "SELECT I.INDEX_NAME, I.IS_GENERATED, IC.COLUMN_NAME, IC.ORDINAL_POSITION, " +
                "IC.ORDERING_SPECIFICATION, IC.IS_UNIQUE " +
                "FROM INFORMATION_SCHEMA.INDEXES I " +
                "JOIN INFORMATION_SCHEMA.INDEX_COLUMNS IC " +
                "ON I.INDEX_CATALOG = IC.INDEX_CATALOG " +
                "AND I.INDEX_SCHEMA = IC.INDEX_SCHEMA " +
                "AND I.INDEX_NAME = IC.INDEX_NAME " +
                "AND I.TABLE_CATALOG = IC.TABLE_CATALOG " +
                "AND I.TABLE_SCHEMA = IC.TABLE_SCHEMA " +
                "AND I.TABLE_NAME = IC.TABLE_NAME " +
                "WHERE I.TABLE_SCHEMA = ? AND I.TABLE_NAME = ? " +
                "ORDER BY I.INDEX_NAME, IC.ORDINAL_POSITION";
        Map<String, MutableIndex> indexes = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    if (indexName == null || columnName == null) {
                        continue;
                    }
                    MutableIndex index = indexes.computeIfAbsent(indexName, name -> new MutableIndex(
                            name,
                            rsBoolean(rs, "IS_UNIQUE"),
                            null,
                            rsBoolean(rs, "IS_GENERATED")
                    ));
                    index.columns.put(rs.getInt("ORDINAL_POSITION"), new IndexColumn(
                            columnName,
                            "DESC".equalsIgnoreCase(rs.getString("ORDERING_SPECIFICATION"))
                    ));
                }
            }
        } catch (SQLException ex) {
            log.debug("Falling back to JDBC index metadata for {}.{}: {}", sourceSchema, table, ex.getMessage());
            indexes.clear();
        }
        return indexes;
    }

    private Map<String, MutableIndex> fetchIndexesFromJdbcMetadata(
            DatabaseMetaData metadata,
            Connection connection,
            String table
    ) throws SQLException {
        Map<String, MutableIndex> indexes = new LinkedHashMap<>();
        try (ResultSet rs = metadata.getIndexInfo(connection.getCatalog(), sourceSchema, table, false, false)) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                if (indexName == null || columnName == null) {
                    continue;
                }
                MutableIndex index = indexes.get(indexName);
                if (index == null) {
                    index = new MutableIndex(
                            indexName,
                            !rsBoolean(rs, "NON_UNIQUE"),
                            safeGetString(rs, "FILTER_CONDITION"),
                            false
                    );
                    indexes.put(indexName, index);
                }
                index.columns.put(rs.getInt("ORDINAL_POSITION"),
                        new IndexColumn(columnName, "D".equalsIgnoreCase(rs.getString("ASC_OR_DESC"))));
            }
        }
        return indexes;
    }

    private List<ForeignKeyDefinition> fetchForeignKeys(
            DatabaseMetaData metadata,
            Connection connection,
            String table
    ) throws SQLException {
        Map<String, MutableForeignKey> keys = new LinkedHashMap<>();
        int unnamed = 0;
        try (ResultSet rs = metadata.getImportedKeys(connection.getCatalog(), sourceSchema, table)) {
            while (rs.next()) {
                String name = rs.getString("FK_NAME");
                if (name == null || name.isBlank()) {
                    name = "FK_" + table + "_" + (++unnamed);
                }
                MutableForeignKey key = keys.get(name);
                if (key == null) {
                    key = new MutableForeignKey(
                            name,
                            rsString(rs, "PKTABLE_SCHEM"),
                            rsString(rs, "PKTABLE_NAME"),
                            rsShort(rs, "UPDATE_RULE"),
                            rsShort(rs, "DELETE_RULE")
                    );
                    keys.put(name, key);
                }
                int position = rs.getInt("KEY_SEQ");
                key.columns.put(position, rs.getString("FKCOLUMN_NAME"));
                key.referencedColumns.put(position, rs.getString("PKCOLUMN_NAME"));
            }
        }
        return keys.values().stream()
                .map(key -> new ForeignKeyDefinition(
                        key.name,
                        orderedValues(key.columns),
                        key.referencedSchema,
                        key.referencedTable,
                        orderedValues(key.referencedColumns),
                        key.updateRule,
                        key.deleteRule
                ))
                .toList();
    }

    private ViewDefinition loadViewDefinition(Connection connection, String view) throws SQLException {
        String sql = "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        String definition;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, view);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("View metadata not found: " + view);
                }
                definition = rs.getString(1);
            }
        }

        List<String> columns = new ArrayList<>();
        String columnSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(columnSql)) {
            ps.setString(1, sourceSchema);
            ps.setString(2, view);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
        }
        return new ViewDefinition(view, columns, definition);
    }

    private void initializeTarget() {
        target.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(targetSchema));
        String table = qualifiedTarget(FAILURE_TABLE);
        target.execute("CREATE TABLE IF NOT EXISTS " + table + " (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY," +
                "\"OBJECT_TYPE\" VARCHAR(32) NOT NULL," +
                "\"OBJECT_NAME\" VARCHAR(512) NOT NULL," +
                "\"ATTEMPT_COUNT\" INTEGER NOT NULL," +
                "\"LAST_ATTEMPT\" TIMESTAMP NOT NULL," +
                "\"ERROR_MESSAGE\" TEXT," +
                "UNIQUE (\"OBJECT_TYPE\", \"OBJECT_NAME\")" +
                ")");
    }

    private void dropSourceViewsFromTarget(Set<String> views) {
        for (String view : views) {
            if (isBlacklisted(view)) {
                continue;
            }
            try {
                target.execute("DROP VIEW IF EXISTS " + qualifiedTarget(view) + " CASCADE");
            } catch (RuntimeException ex) {
                log.debug("Ignoring pre-refresh view drop failure for {}: {}", view, ex.getMessage());
            }
        }
    }

    private void syncSequence(SequenceDefinition sequence) {
        String qualified = qualifiedTarget(sequence.name());
        target.execute("DROP SEQUENCE IF EXISTS " + qualified + (h2TestTarget ? "" : " CASCADE"));
        StringBuilder ddl = new StringBuilder("CREATE SEQUENCE ").append(qualified)
                .append(" START WITH ").append(sequence.baseValue().toPlainString())
                .append(" INCREMENT BY ").append(sequence.increment().toPlainString());
        if (sequence.minimumValue() != null) {
            ddl.append(" MINVALUE ").append(sequence.minimumValue().toPlainString());
        }
        if (sequence.maximumValue() != null) {
            ddl.append(" MAXVALUE ").append(sequence.maximumValue().toPlainString());
        }
        ddl.append(sequence.cycle() ? " CYCLE" : " NO CYCLE");
        if (sequence.cache() > 0) {
            ddl.append(" CACHE ").append(sequence.cache());
        }
        target.execute(ddl.toString());
        log.info("Synced PostgreSQL sequence {} nextValue={}", sequence.name(), sequence.baseValue());
    }

    private void copyTable(TableDefinition table) {
        log.info("Copying H2 table {}.{} to PostgreSQL {}.{}",
                sourceSchema, table.name(), targetSchema, table.name());
        createTargetTable(table);
        bulkInsert(table);
        syncIdentityNextValues(table);
    }

    private void createTargetTable(TableDefinition table) {
        String qualified = qualifiedTarget(table.name());
        target.execute("DROP TABLE IF EXISTS " + qualified + " CASCADE");

        List<String> parts = new ArrayList<>();
        for (ColumnDefinition column : table.columns()) {
            StringBuilder definition = new StringBuilder(quoteIdentifier(column.name()))
                    .append(' ')
                    .append(mapType(column));
            if (column.generated()) {
                if (column.generationExpression() == null || column.generationExpression().isBlank()) {
                    throw new IllegalStateException("Generated column expression is unavailable for " +
                            table.name() + "." + column.name());
                }
                definition.append(" GENERATED ALWAYS AS (")
                        .append(viewSqlTranslator.translate(column.generationExpression()))
                        .append(')');
                if (!h2TestTarget) {
                    definition.append(" STORED");
                }
            } else if (column.autoIncrement()) {
                definition.append(" GENERATED BY DEFAULT AS IDENTITY")
                        .append(identityOptions(column));
            } else {
                String translatedDefault = translateDefault(column.defaultValue());
                if (translatedDefault != null) {
                    definition.append(" DEFAULT ").append(translatedDefault);
                }
            }
            if (!column.nullable() || isPrimaryKeyColumn(table, column.name())) {
                definition.append(" NOT NULL");
            }
            parts.add(definition.toString());
        }

        target.execute("CREATE TABLE " + qualified + " (" + String.join(", ", parts) + ")");
    }

    private void syncPrimaryKey(TableDefinition table, ConstraintDefinition primaryKey) {
        dropTargetConstraint(table.name(), primaryKey.name());
        target.execute("ALTER TABLE " + qualifiedTarget(table.name()) +
                " ADD CONSTRAINT " + quoteIdentifier(primaryKey.name()) +
                " PRIMARY KEY (" + quoteIdentifiers(primaryKey.columns()) + ")");
        log.info("Created PostgreSQL primary key {} on {}", primaryKey.name(), table.name());
    }

    private void syncUniqueConstraint(TableDefinition table, ConstraintDefinition unique) {
        dropTargetConstraint(table.name(), unique.name());
        target.execute("ALTER TABLE " + qualifiedTarget(table.name()) +
                " ADD CONSTRAINT " + quoteIdentifier(unique.name()) +
                " UNIQUE (" + quoteIdentifiers(unique.columns()) + ")");
        log.info("Created PostgreSQL unique constraint {} on {}", unique.name(), table.name());
    }

    private void dropTargetConstraint(String table, String constraint) {
        target.execute("ALTER TABLE " + qualifiedTarget(table) +
                " DROP CONSTRAINT IF EXISTS " + quoteIdentifier(constraint));
    }

    private boolean isPrimaryKeyColumn(TableDefinition table, String column) {
        return table.primaryKey() != null && table.primaryKey().columns().contains(column);
    }

    private String identityOptions(ColumnDefinition column) {
        List<String> options = new ArrayList<>();
        if (column.identityStart() != null) {
            options.add("START WITH " + column.identityStart().toPlainString());
        }
        if (column.identityIncrement() != null) {
            options.add("INCREMENT BY " + column.identityIncrement().toPlainString());
        }
        if (column.identityMinimum() != null) {
            options.add("MINVALUE " + column.identityMinimum().toPlainString());
        }
        if (column.identityMaximum() != null) {
            options.add("MAXVALUE " + column.identityMaximum().toPlainString());
        }
        options.add(column.identityCycle() ? "CYCLE" : "NO CYCLE");
        if (column.identityCache() > 0) {
            options.add("CACHE " + column.identityCache());
        }
        return options.isEmpty() ? "" : " (" + String.join(" ", options) + ")";
    }

    private void syncIdentityNextValues(TableDefinition table) {
        for (ColumnDefinition column : table.columns()) {
            if (!column.autoIncrement() || column.identityBase() == null) {
                continue;
            }
            if (h2TestTarget) {
                target.execute("ALTER TABLE " + qualifiedTarget(table.name()) +
                        " ALTER COLUMN " + quoteIdentifier(column.name()) +
                        " RESTART WITH " + column.identityBase().toPlainString());
            } else {
                String tableRegclass = quoteIdentifier(targetSchema) + "." + quoteIdentifier(table.name());
                target.queryForObject(
                        "SELECT setval(pg_get_serial_sequence(?, ?), CAST(? AS BIGINT), false)",
                        Long.class,
                        tableRegclass,
                        column.name(),
                        column.identityBase().longValueExact()
                );
            }
            log.info("Aligned identity {}.{} nextValue={}",
                    table.name(), column.name(), column.identityBase());
        }
    }

    private String translateDefault(String defaultValue) {
        if (defaultValue == null || defaultValue.isBlank() || "NULL".equalsIgnoreCase(defaultValue.trim())) {
            return null;
        }
        String trimmed = defaultValue.trim();
        Matcher sequenceMatcher = NEXT_VALUE_PATTERN.matcher(trimmed);
        if (sequenceMatcher.matches()) {
            String sequenceName = firstNonNull(sequenceMatcher.group(3), sequenceMatcher.group(4));
            String regclass = quoteIdentifier(targetSchema) + "." + quoteIdentifier(sequenceName);
            return "nextval('" + regclass.replace("'", "''") + "'::regclass)";
        }
        return viewSqlTranslator.translate(trimmed);
    }

    private String mapType(ColumnDefinition column) {
        int jdbcType = column.jdbcType();
        String typeName = column.typeName() == null ? "" : column.typeName().toUpperCase(Locale.ROOT);
        int precision = column.size();
        int scale = column.scale();

        if (typeName.contains("UUID")) return "UUID";
        if (typeName.contains("JSON")) return "JSONB";
        if (typeName.contains("INTERVAL")) return "INTERVAL";
        if (jdbcType == Types.ARRAY || typeName.endsWith(" ARRAY")) {
            return mapArrayType(typeName);
        }

        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT -> "SMALLINT";
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.NUMERIC, Types.DECIMAL -> {
                if (precision <= 0 || precision > 1000 || scale < 0 || scale > precision) {
                    yield "NUMERIC";
                }
                yield "NUMERIC(" + precision + "," + scale + ")";
            }
            case Types.REAL -> "REAL";
            case Types.FLOAT, Types.DOUBLE -> "DOUBLE PRECISION";
            case Types.BOOLEAN, Types.BIT -> "BOOLEAN";
            case Types.DATE -> "DATE";
            case Types.TIME -> "TIME";
            case Types.TIME_WITH_TIMEZONE -> "TIME WITH TIME ZONE";
            case Types.TIMESTAMP -> "TIMESTAMP";
            case Types.TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP WITH TIME ZONE";
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> "BYTEA";
            case Types.CLOB, Types.NCLOB, Types.LONGVARCHAR, Types.LONGNVARCHAR -> "TEXT";
            case Types.CHAR, Types.NCHAR -> precision > 0 && precision <= POSTGRESQL_MAX_VARCHAR_LENGTH
                    ? "CHAR(" + precision + ")" : "TEXT";
            case Types.VARCHAR, Types.NVARCHAR -> precision > 0 && precision <= POSTGRESQL_MAX_VARCHAR_LENGTH
                    ? "VARCHAR(" + precision + ")" : "TEXT";
            case Types.JAVA_OBJECT, Types.OTHER -> "TEXT";
            default -> "TEXT";
        };
    }

    private String mapArrayType(String h2TypeName) {
        String base = h2TypeName.replaceFirst("(?i)\\s+ARRAY$", "").trim();
        if (base.contains("BIGINT")) return "BIGINT[]";
        if (base.contains("SMALLINT") || base.contains("TINYINT")) return "SMALLINT[]";
        if (base.contains("INTEGER")) return "INTEGER[]";
        if (base.contains("DOUBLE") || base.contains("FLOAT")) return "DOUBLE PRECISION[]";
        if (base.contains("REAL")) return "REAL[]";
        if (base.contains("NUMERIC") || base.contains("DECIMAL")) return "NUMERIC[]";
        if (base.contains("BOOLEAN")) return "BOOLEAN[]";
        if (base.contains("UUID")) return "UUID[]";
        if (base.contains("TIMESTAMP")) return "TIMESTAMP[]";
        if (base.equals("DATE")) return "DATE[]";
        return "TEXT[]";
    }

    private void bulkInsert(TableDefinition table) {
        List<ColumnDefinition> insertable = table.columns().stream()
                .filter(column -> !column.generated())
                .toList();
        boolean hasLob = insertable.stream().anyMatch(this::isLobColumn);
        String sourceTable = qualifiedSource(table.name());
        String targetTable = qualifiedTarget(table.name());

        if (insertable.isEmpty()) {
            long count = source.queryForObject("SELECT COUNT(*) FROM " + sourceTable, Long.class);
            for (long i = 0; i < count; i++) {
                target.execute("INSERT INTO " + targetTable + " DEFAULT VALUES");
            }
            log.info("Inserted {} rows into {}", count, targetTable);
            return;
        }

        String columns = insertable.stream().map(ColumnDefinition::name)
                .map(this::quoteIdentifier).collect(Collectors.joining(","));
        String selectSql = "SELECT " + columns + " FROM " + sourceTable;
        String placeholders = insertable.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String insertSql = "INSERT INTO " + targetTable + " (" + columns + ") VALUES (" + placeholders + ")";

        long total = 0;
        try {
            Long count = source.queryForObject("SELECT COUNT(*) FROM " + sourceTable, Long.class);
            total = count == null ? 0 : count;
        } catch (RuntimeException ex) {
            log.warn("Count failed (non-fatal) for {}: {}", sourceTable, ex.getMessage());
        }

        try (Connection sourceConnection = sourceDataSource.getConnection();
             PreparedStatement select = sourceConnection.prepareStatement(
                     selectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             Connection targetConnection = targetDataSource.getConnection()) {
            select.setFetchSize(Math.max(batchSize, 100));
            targetConnection.setAutoCommit(false);
            try (ResultSet rs = select.executeQuery();
                 PreparedStatement insert = targetConnection.prepareStatement(insertSql)) {
                long copied = 0;
                while (rs.next()) {
                    List<Object> values = new ArrayList<>(insertable.size());
                    List<AutoCloseable> rowResources = new ArrayList<>();
                    try {
                        for (int i = 0; i < insertable.size(); i++) {
                            values.add(readH2Value(rs, i + 1, insertable.get(i)));
                        }
                        for (int i = 0; i < insertable.size(); i++) {
                            bindTargetValue(
                                    targetConnection,
                                    insert,
                                    i + 1,
                                    values.get(i),
                                    insertable.get(i),
                                    rowResources
                            );
                        }
                        if (hasLob) {
                            // Execute immediately so streams are consumed before the H2 cursor advances.
                            insert.executeUpdate();
                            insert.clearParameters();
                        } else {
                            insert.addBatch();
                        }
                        copied++;
                        if (copied % batchSize == 0) {
                            if (!hasLob) {
                                insert.executeBatch();
                            }
                            targetConnection.commit();
                            log.info("Inserted {} / {} rows into {}", copied, total, targetTable);
                        }
                    } finally {
                        closeResources(rowResources);
                        freeLobValues(values);
                    }
                }
                if (!hasLob) {
                    insert.executeBatch();
                }
                targetConnection.commit();
                log.info("Inserted {} rows into {}", copied, targetTable);
            } catch (SQLException ex) {
                targetConnection.rollback();
                throw ex;
            } finally {
                targetConnection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Bulk insert failed for target " + targetTable, ex);
        }
    }

    private Object readH2Value(ResultSet rs, int index, ColumnDefinition column) throws SQLException {
        return switch (column.jdbcType()) {
            case Types.BLOB -> rs.getBlob(index);
            case Types.CLOB, Types.NCLOB -> rs.getClob(index);
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> rs.getBytes(index);
            case Types.LONGVARCHAR, Types.LONGNVARCHAR -> rs.getString(index);
            case Types.ARRAY -> readSqlArray(rs.getArray(index));
            default -> normalizeJdbcValue(rs.getObject(index), column);
        };
    }

    private Object normalizeJdbcValue(Object value, ColumnDefinition column) throws SQLException {
        if (value instanceof Blob blob) {
            try {
                return blob.getBytes(1, Math.toIntExact(blob.length()));
            } finally {
                blob.free();
            }
        }
        if (value instanceof Clob clob) {
            try {
                return clob.getSubString(1, Math.toIntExact(clob.length()));
            } finally {
                clob.free();
            }
        }
        if (value instanceof java.sql.Array array) {
            return readSqlArray(array);
        }
        if (value instanceof byte[] bytes && column.typeName() != null
                && column.typeName().toUpperCase(Locale.ROOT).contains("JSON")) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (column.jdbcType() == Types.JAVA_OBJECT || column.jdbcType() == Types.OTHER) {
            return value.toString();
        }
        return value;
    }

    private Object[] readSqlArray(java.sql.Array array) throws SQLException {
        if (array == null) return null;
        try {
            Object value = array.getArray();
            if (value instanceof Object[] objects) {
                return objects;
            }
            int length = Array.getLength(value);
            Object[] objects = new Object[length];
            for (int i = 0; i < length; i++) {
                objects[i] = Array.get(value, i);
            }
            return objects;
        } finally {
            array.free();
        }
    }

    private void bindTargetValue(
            Connection targetConnection,
            PreparedStatement statement,
            int index,
            Object value,
            ColumnDefinition column,
            List<AutoCloseable> resources
    ) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
            return;
        }
        if (value instanceof Blob blob) {
            InputStream stream = blob.getBinaryStream();
            statement.setBinaryStream(index, stream, blob.length());
            resources.add(stream);
            return;
        }
        if (value instanceof Clob clob) {
            Reader reader = clob.getCharacterStream();
            statement.setCharacterStream(index, reader, clob.length());
            resources.add(reader);
            return;
        }
        if (column.jdbcType() == Types.ARRAY && value instanceof Object[] values) {
            statement.setArray(index, targetConnection.createArrayOf(postgresqlArrayElementType(column), values));
            return;
        }
        if (!h2TestTarget && column.typeName() != null
                && column.typeName().toUpperCase(Locale.ROOT).contains("JSON")) {
            statement.setObject(index, value.toString(), Types.OTHER);
            return;
        }
        if (column.jdbcType() == Types.JAVA_OBJECT || column.jdbcType() == Types.OTHER) {
            statement.setString(index, value.toString());
            return;
        }
        statement.setObject(index, value);
    }

    private boolean isLobColumn(ColumnDefinition column) {
        return column.jdbcType() == Types.BLOB
                || column.jdbcType() == Types.CLOB
                || column.jdbcType() == Types.NCLOB;
    }

    private void closeResources(List<AutoCloseable> resources) {
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception ex) {
                log.debug("Failed to close streamed LOB resource: {}", ex.getMessage());
            }
        }
    }

    private void freeLobValues(List<Object> values) {
        for (Object value : values) {
            try {
                if (value instanceof Blob blob) {
                    blob.free();
                } else if (value instanceof Clob clob) {
                    clob.free();
                }
            } catch (SQLException ex) {
                log.debug("Failed to free H2 LOB value: {}", ex.getMessage());
            }
        }
    }

    private String postgresqlArrayElementType(ColumnDefinition column) {
        String mapped = mapArrayType(column.typeName() == null ? "" : column.typeName());
        return mapped.substring(0, mapped.length() - 2);
    }

    private void syncIndex(TableDefinition table, IndexDefinition index) {
        String qualifiedIndex = quoteIdentifier(targetSchema) + "." + quoteIdentifier(index.name());
        target.execute("DROP INDEX IF EXISTS " + qualifiedIndex);
        String columns = index.columns().stream()
                .map(column -> quoteIdentifier(column.name()) + (column.descending() ? " DESC" : " ASC"))
                .collect(Collectors.joining(", "));
        StringBuilder ddl = new StringBuilder("CREATE ");
        if (index.unique()) {
            ddl.append("UNIQUE ");
        }
        ddl.append("INDEX ").append(qualifiedIndex)
                .append(" ON ").append(qualifiedTarget(table.name()))
                .append(" (").append(columns).append(')');
        if (index.filterCondition() != null && !index.filterCondition().isBlank()) {
            ddl.append(" WHERE ").append(viewSqlTranslator.translate(index.filterCondition()));
        }
        target.execute(ddl.toString());
        log.info("Created PostgreSQL index {} on {}", index.name(), table.name());
    }

    private void syncForeignKey(TableDefinition table, ForeignKeyDefinition foreignKey) {
        dropTargetConstraint(table.name(), foreignKey.name());
        String ddl = "ALTER TABLE " + qualifiedTarget(table.name()) +
                " ADD CONSTRAINT " + quoteIdentifier(foreignKey.name()) +
                " FOREIGN KEY (" + quoteIdentifiers(foreignKey.columns()) + ")" +
                " REFERENCES " + qualifiedTarget(foreignKey.referencedTable()) +
                " (" + quoteIdentifiers(foreignKey.referencedColumns()) + ")" +
                ruleSql("UPDATE", foreignKey.updateRule()) +
                ruleSql("DELETE", foreignKey.deleteRule());
        target.execute(ddl);
        log.info("Created PostgreSQL foreign key {} on {}", foreignKey.name(), table.name());
    }

    private String ruleSql(String operation, short rule) {
        String action = switch (rule) {
            case DatabaseMetaData.importedKeyCascade -> "CASCADE";
            case DatabaseMetaData.importedKeySetNull -> "SET NULL";
            case DatabaseMetaData.importedKeySetDefault -> "SET DEFAULT";
            case DatabaseMetaData.importedKeyRestrict -> "RESTRICT";
            default -> "NO ACTION";
        };
        return " ON " + operation + " " + action;
    }

    private void syncViewsWithDependencyAwareness(
            Map<String, ViewDefinition> views,
            List<MigrationFailure> failures
    ) {
        Deque<ViewDefinition> queue = new ArrayDeque<>(views.values());
        Map<String, Integer> attempts = new HashMap<>();
        int deferralLimit = Math.max(3, maxRetries) * 4;

        while (!queue.isEmpty()) {
            ViewDefinition view = queue.removeFirst();
            int attempt = attempts.merge(view.name(), 1, Integer::sum);
            try {
                syncView(view);
                recordSuccess("VIEW", view.name());
            } catch (RuntimeException ex) {
                if (isMissingDependency(ex) && attempt <= deferralLimit) {
                    log.info("Deferring PostgreSQL view {} until dependencies exist (attempt {}). Cause: {}",
                            view.name(), attempt, truncate(extractMessage(ex), 512));
                    queue.addLast(view);
                } else {
                    recordTerminalFailure("VIEW", view.name(), attempt, ex, failures);
                    log.warn("Giving up on PostgreSQL view {} after {} attempts: {}",
                            view.name(), attempt, ex.toString());
                }
            }
        }
    }

    private void syncView(ViewDefinition view) {
        String qualified = qualifiedTarget(view.name());
        target.execute("DROP VIEW IF EXISTS " + qualified + " CASCADE");
        String columnList = view.columns().isEmpty() ? "" :
                view.columns().stream().map(this::quoteIdentifier)
                        .collect(Collectors.joining(", ", " (", ")"));
        target.execute("CREATE VIEW " + qualified + columnList + " AS " +
                viewSqlTranslator.translate(view.sql()));
        log.info("Created PostgreSQL view {}", view.name());
    }

    private boolean isMissingDependency(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException sqlException) {
                String state = sqlException.getSQLState();
                if ("42P01".equals(state) || "3F000".equals(state) || "42S02".equals(state)) {
                    return true;
                }
                String message = sqlException.getMessage();
                if (message != null && message.toLowerCase(Locale.ROOT).contains("not found")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean retry(
            Runnable task,
            String type,
            String name,
            List<MigrationFailure> failures
    ) {
        int attempt = 0;
        while (true) {
            try {
                attempt++;
                task.run();
                recordSuccess(type, name);
                return true;
            } catch (Exception ex) {
                log.warn("Failed to process PostgreSQL {} {} on attempt {}: {}",
                        type, name, attempt, ex.toString());
                if (attempt >= maxRetries) {
                    recordTerminalFailure(type, name, attempt, ex, failures);
                    return false;
                }
                try {
                    Thread.sleep(1000L * attempt * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    recordTerminalFailure(type, name, attempt, interrupted, failures);
                    return false;
                }
            }
        }
    }

    private void waitForFutures(
            List<Future<?>> futures,
            String objectType,
            List<MigrationFailure> failures
    ) {
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for PostgreSQL " + objectType + " tasks", ex);
                } catch (ExecutionException ex) {
                    Throwable cause = ex.getCause() == null ? ex : ex.getCause();
                    log.error("Unexpected failure while processing PostgreSQL {} tasks", objectType, cause);
                    failures.add(new MigrationFailure(
                            "INTERNAL_" + objectType.toUpperCase(Locale.ROOT),
                            objectType,
                            extractMessage(cause)
                    ));
                }
            }
        } finally {
            futures.clear();
        }
    }

    private void recordTerminalFailure(
            String type,
            String name,
            int attempt,
            Exception ex,
            List<MigrationFailure> failures
    ) {
        try {
            recordFailure(type, name, attempt, ex);
        } catch (RuntimeException logFailure) {
            ex.addSuppressed(logFailure);
            log.error("Failed to record terminal migration failure for {} {}", type, name, logFailure);
        }
        failures.add(new MigrationFailure(type, name, extractMessage(ex)));
    }

    private void recordSuccess(String type, String name) {
        if (h2TestTarget) {
            target.update("MERGE INTO " + qualifiedTarget(FAILURE_TABLE) +
                            " (\"OBJECT_TYPE\", \"OBJECT_NAME\", \"ATTEMPT_COUNT\", \"LAST_ATTEMPT\", \"ERROR_MESSAGE\") " +
                            "KEY (\"OBJECT_TYPE\", \"OBJECT_NAME\") VALUES (?, ?, 0, CURRENT_TIMESTAMP, NULL)",
                    type, name);
            return;
        }
        target.update("INSERT INTO " + qualifiedTarget(FAILURE_TABLE) +
                        " (\"OBJECT_TYPE\", \"OBJECT_NAME\", \"ATTEMPT_COUNT\", \"LAST_ATTEMPT\", \"ERROR_MESSAGE\") " +
                        "VALUES (?, ?, 0, CURRENT_TIMESTAMP, NULL) " +
                        "ON CONFLICT (\"OBJECT_TYPE\", \"OBJECT_NAME\") DO UPDATE SET " +
                        "\"ATTEMPT_COUNT\" = 0, \"LAST_ATTEMPT\" = EXCLUDED.\"LAST_ATTEMPT\", \"ERROR_MESSAGE\" = NULL",
                type, name);
    }

    private void recordFailure(String type, String name, int attempt, Exception ex) {
        String error = truncate(exceptionToString(ex), 16_000);
        if (h2TestTarget) {
            target.update("MERGE INTO " + qualifiedTarget(FAILURE_TABLE) +
                            " (\"OBJECT_TYPE\", \"OBJECT_NAME\", \"ATTEMPT_COUNT\", \"LAST_ATTEMPT\", \"ERROR_MESSAGE\") " +
                            "KEY (\"OBJECT_TYPE\", \"OBJECT_NAME\") VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)",
                    type, name, attempt, error);
            return;
        }
        target.update("INSERT INTO " + qualifiedTarget(FAILURE_TABLE) +
                        " (\"OBJECT_TYPE\", \"OBJECT_NAME\", \"ATTEMPT_COUNT\", \"LAST_ATTEMPT\", \"ERROR_MESSAGE\") " +
                        "VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?) " +
                        "ON CONFLICT (\"OBJECT_TYPE\", \"OBJECT_NAME\") DO UPDATE SET " +
                        "\"ATTEMPT_COUNT\" = EXCLUDED.\"ATTEMPT_COUNT\", " +
                        "\"LAST_ATTEMPT\" = EXCLUDED.\"LAST_ATTEMPT\", " +
                        "\"ERROR_MESSAGE\" = EXCLUDED.\"ERROR_MESSAGE\"",
                type, name, attempt, error);
    }

    private boolean isBlacklisted(String name) {
        if (name == null) return false;
        String normalized = name.toUpperCase(Locale.ROOT);
        return blacklist.contains(normalized)
                || blacklist.contains(sourceSchema.toUpperCase(Locale.ROOT) + "." + normalized);
    }

    private boolean isSourceSchema(String schema) {
        return schema == null || schema.isBlank() || sourceSchema.equalsIgnoreCase(schema);
    }

    private String qualifiedSource(String object) {
        return quoteIdentifier(sourceSchema) + "." + quoteIdentifier(object);
    }

    private String qualifiedTarget(String object) {
        return quoteIdentifier(targetSchema) + "." + quoteIdentifier(object);
    }

    private String quoteIdentifiers(List<String> identifiers) {
        return identifiers.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + requireIdentifier(identifier, "identifier").replace("\"", "\"\"") + "\"";
    }

    private static String requireIdentifier(String identifier, String label) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        return identifier.trim();
    }

    private static Set<String> parseBlacklist(String csv) {
        return Arrays.stream((csv == null ? "" : csv).split(","))
                .map(String::trim)
                .map(value -> value.replaceAll("^[\\\"']|[\\\"']$", ""))
                .filter(value -> !value.isBlank())
                .map(value -> value.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static boolean detectH2Target(DataSource dataSource) {
        if (dataSource instanceof DriverManagerDataSource driverManagerDataSource) {
            String url = driverManagerDataSource.getUrl();
            return url != null && url.toLowerCase(Locale.ROOT).startsWith("jdbc:h2:");
        }
        // Do not connect to a disabled/placeholder PostgreSQL destination at
        // application startup. Production behavior is the PostgreSQL branch.
        return false;
    }

    private static String safeGetString(ResultSet rs, String column) {
        try {
            return rs.getString(column);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static boolean rsBoolean(ResultSet rs, String column) {
        try {
            return rs.getBoolean(column);
        } catch (SQLException ex) {
            return false;
        }
    }

    private static String rsString(ResultSet rs, String column) {
        try {
            return rs.getString(column);
        } catch (SQLException ex) {
            return null;
        }
    }

    private static short rsShort(ResultSet rs, String column) {
        try {
            return rs.getShort(column);
        } catch (SQLException ex) {
            return DatabaseMetaData.importedKeyNoAction;
        }
    }

    private static <T> List<T> orderedValues(Map<Integer, T> values) {
        return values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    private static String firstNonNull(String first, String second) {
        return first != null ? first : second;
    }

    private static String extractMessage(Throwable ex) {
        if (ex == null) return "";
        if (ex.getMessage() != null && !ex.getMessage().isBlank()) return ex.getMessage();
        return ex.getCause() == null ? ex.toString() : extractMessage(ex.getCause());
    }

    private static String exceptionToString(Exception ex) {
        StringBuilder out = new StringBuilder(ex.toString()).append('\n');
        for (StackTraceElement element : ex.getStackTrace()) {
            out.append("  at ").append(element).append('\n');
        }
        return out.toString();
    }

    private static String truncate(String value, int max) {
        return value == null || value.length() <= max ? value : value.substring(0, max);
    }

    public static final class MigrationIncompleteException extends RuntimeException {
        private final int failureCount;

        private MigrationIncompleteException(List<MigrationFailure> failures) {
            super("H2 -> PostgreSQL migration completed with " + failures.size() +
                    " failed objects: " + failures.stream()
                    .map(failure -> failure.type() + " " + failure.name())
                    .limit(10)
                    .collect(Collectors.joining(", ")));
            this.failureCount = failures.size();
        }

        public int getFailureCount() {
            return failureCount;
        }
    }

    record MigrationFailure(String type, String name, String message) {
    }

    record MigrationSnapshot(
            Set<String> tableNames,
            Set<String> viewNames,
            Map<String, TableDefinition> tables,
            Map<String, ViewDefinition> views,
            List<SequenceDefinition> sequences
    ) {
    }

    record TableDefinition(
            String name,
            List<ColumnDefinition> columns,
            ConstraintDefinition primaryKey,
            List<ConstraintDefinition> uniqueConstraints,
            List<IndexDefinition> indexes,
            List<ForeignKeyDefinition> foreignKeys
    ) {
    }

    record ColumnDefinition(
            String name,
            int jdbcType,
            String typeName,
            int size,
            int scale,
            boolean nullable,
            boolean autoIncrement,
            boolean generated,
            String defaultValue,
            String generationExpression,
            BigDecimal identityStart,
            BigDecimal identityIncrement,
            BigDecimal identityMinimum,
            BigDecimal identityMaximum,
            BigDecimal identityBase,
            boolean identityCycle,
            long identityCache
    ) {
    }

    private record ColumnExtras(
            String generationExpression,
            BigDecimal identityStart,
            BigDecimal identityIncrement,
            BigDecimal identityMinimum,
            BigDecimal identityMaximum,
            BigDecimal identityBase,
            boolean identityCycle,
            long identityCache
    ) {
        private static final ColumnExtras EMPTY = new ColumnExtras(
                null, null, null, null, null, null, false, 0
        );
    }

    record ConstraintDefinition(String name, List<String> columns) {
    }

    record IndexColumn(String name, boolean descending) {
    }

    record IndexDefinition(
            String name,
            boolean unique,
            List<IndexColumn> columns,
            String filterCondition
    ) {
    }

    record ForeignKeyDefinition(
            String name,
            List<String> columns,
            String referencedSchema,
            String referencedTable,
            List<String> referencedColumns,
            short updateRule,
            short deleteRule
    ) {
    }

    record ViewDefinition(String name, List<String> columns, String sql) {
    }

    record SequenceDefinition(
            String name,
            BigDecimal increment,
            BigDecimal baseValue,
            BigDecimal minimumValue,
            BigDecimal maximumValue,
            boolean cycle,
            long cache
    ) {
    }

    private static final class MutableIndex {
        private final String name;
        private final boolean unique;
        private final Map<Integer, IndexColumn> columns = new LinkedHashMap<>();
        private final String filterCondition;
        private final boolean generated;

        private MutableIndex(String name, boolean unique, String filterCondition, boolean generated) {
            this.name = name;
            this.unique = unique;
            this.filterCondition = filterCondition;
            this.generated = generated;
        }
    }

    private static final class MutableForeignKey {
        private final String name;
        private final String referencedSchema;
        private final String referencedTable;
        private final short updateRule;
        private final short deleteRule;
        private final Map<Integer, String> columns = new LinkedHashMap<>();
        private final Map<Integer, String> referencedColumns = new LinkedHashMap<>();

        private MutableForeignKey(
                String name,
                String referencedSchema,
                String referencedTable,
                short updateRule,
                short deleteRule
        ) {
            this.name = name;
            this.referencedSchema = referencedSchema;
            this.referencedTable = referencedTable;
            this.updateRule = updateRule;
            this.deleteRule = deleteRule;
        }
    }
}
