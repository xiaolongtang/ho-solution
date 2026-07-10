package com.example.h2sync.service;

import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import static com.example.h2sync.service.H2ToPostgresqlLoaderService.ForeignKeyDefinition;
import static com.example.h2sync.service.H2ToPostgresqlLoaderService.IndexDefinition;
import static com.example.h2sync.service.H2ToPostgresqlLoaderService.MigrationSnapshot;
import static com.example.h2sync.service.H2ToPostgresqlLoaderService.SequenceDefinition;
import static com.example.h2sync.service.H2ToPostgresqlLoaderService.TableDefinition;

/** Builds the post-load comparison report printed after every full refresh. */
final class H2ToPostgresqlMigrationReportPrinter {

    private final Logger log;
    private final JdbcTemplate source;
    private final JdbcTemplate target;
    private final DataSource targetDataSource;
    private final String sourceSchema;
    private final String targetSchema;
    private final Predicate<String> blacklist;
    private final boolean h2TestTarget;

    H2ToPostgresqlMigrationReportPrinter(
            Logger log,
            JdbcTemplate source,
            JdbcTemplate target,
            DataSource targetDataSource,
            String sourceSchema,
            String targetSchema,
            Predicate<String> blacklist,
            boolean h2TestTarget
    ) {
        this.log = log;
        this.source = source;
        this.target = target;
        this.targetDataSource = targetDataSource;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.blacklist = blacklist;
        this.h2TestTarget = h2TestTarget;
    }

    void printReport(MigrationSnapshot snapshot) {
        List<String[]> tableRows = new ArrayList<>();
        for (String table : snapshot.tableNames()) {
            tableRows.add(buildTableRow(table));
        }

        List<String[]> viewRows = new ArrayList<>();
        for (String view : snapshot.viewNames()) {
            viewRows.add(new String[]{view, objectStatus(view, "VIEW", view)});
        }

        List<String[]> sequenceRows = snapshot.sequences().stream()
                .sorted(Comparator.comparing(SequenceDefinition::name))
                .map(this::buildSequenceRow)
                .toList();

        List<String[]> indexRows = new ArrayList<>();
        List<String[]> constraintRows = new ArrayList<>();
        List<String[]> foreignKeyRows = new ArrayList<>();
        for (TableDefinition table : snapshot.tables().values()) {
            if (table.primaryKey() != null) {
                constraintRows.add(new String[]{table.name(), "PRIMARY KEY", table.primaryKey().name(),
                        constraintStatus(table.name(), table.primaryKey().name())});
            }
            for (var unique : table.uniqueConstraints()) {
                constraintRows.add(new String[]{table.name(), "UNIQUE", unique.name(),
                        constraintStatus(table.name(), unique.name())});
            }
            for (IndexDefinition index : table.indexes()) {
                indexRows.add(new String[]{table.name(), index.name(),
                        objectStatus(index.name(), "INDEX", table.name())});
            }
            for (ForeignKeyDefinition key : table.foreignKeys()) {
                String status;
                if ((key.referencedSchema() != null && !sourceSchema.equalsIgnoreCase(key.referencedSchema()))
                        || blacklist.test(key.referencedTable())
                        || !snapshot.tables().containsKey(key.referencedTable())) {
                    status = "SKIPPED";
                } else {
                    status = objectStatus(key.name(), "FOREIGN_KEY", table.name());
                }
                foreignKeyRows.add(new String[]{table.name(), key.name(), status});
            }
        }

        StringBuilder report = new StringBuilder();
        report.append("================ H2 -> POSTGRESQL MIGRATION REPORT ================\n\n");
        report.append(renderSection("Tables",
                new String[]{"Table", "H2 Rows", "PostgreSQL Rows", "Status"}, tableRows)).append('\n');
        report.append(renderSection("Views", new String[]{"View", "Status"}, viewRows)).append('\n');
        report.append(renderSection("Sequences",
                new String[]{"Sequence", "H2 Next", "PostgreSQL Next", "Status"}, sequenceRows)).append('\n');
        report.append(renderSection("Constraints",
                new String[]{"Table", "Type", "Constraint", "Status"}, constraintRows)).append('\n');
        report.append(renderSection("Indexes", new String[]{"Table", "Index", "Status"}, indexRows)).append('\n');
        report.append(renderSection("Foreign Keys",
                new String[]{"Table", "Foreign Key", "Status"}, foreignKeyRows));
        report.append("===================================================================");
        log.info("\n{}", report);
    }

    private String[] buildTableRow(String table) {
        if (blacklist.test(table)) {
            return new String[]{table, "-", "-", "SKIPPED"};
        }
        NumericResult h2Rows = count(source, sourceSchema, table);
        NumericResult postgresqlRows = count(target, targetSchema, table);
        String status = !h2Rows.success() || !postgresqlRows.success()
                ? "ERROR"
                : difference(postgresqlRows.value().subtract(h2Rows.value()));
        return new String[]{table, h2Rows.display(), postgresqlRows.display(), status};
    }

    private String[] buildSequenceRow(SequenceDefinition sequence) {
        if (blacklist.test(sequence.name())) {
            return new String[]{sequence.name(), "-", "-", "SKIPPED"};
        }
        NumericResult targetValue = targetSequenceValue(sequence.name());
        String status = !targetValue.success()
                ? "ERROR"
                : difference(targetValue.value().subtract(sequence.baseValue()));
        return new String[]{
                sequence.name(),
                number(sequence.baseValue()),
                targetValue.display(),
                status
        };
    }

    private NumericResult count(JdbcTemplate jdbc, String schema, String table) {
        try {
            Long count = jdbc.queryForObject("SELECT COUNT(*) FROM " + qualified(schema, table), Long.class);
            return count == null ? NumericResult.error("NO DATA") : NumericResult.success(BigDecimal.valueOf(count));
        } catch (DataAccessException ex) {
            return NumericResult.error(message(ex));
        }
    }

    private NumericResult targetSequenceValue(String sequence) {
        try {
            BigDecimal value;
            if (h2TestTarget) {
                value = target.queryForObject(
                        "SELECT BASE_VALUE FROM INFORMATION_SCHEMA.SEQUENCES " +
                                "WHERE SEQUENCE_SCHEMA = ? AND SEQUENCE_NAME = ?",
                        BigDecimal.class,
                        targetSchema,
                        sequence
                );
            } else {
                value = target.queryForObject(
                        "SELECT last_value FROM " + qualified(targetSchema, sequence),
                        BigDecimal.class
                );
            }
            return value == null ? NumericResult.error("NO DATA") : NumericResult.success(value);
        } catch (DataAccessException ex) {
            return NumericResult.error(message(ex));
        }
    }

    private String constraintStatus(String table, String constraint) {
        try {
            Integer count = target.queryForObject(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " +
                            "WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = ?",
                    Integer.class,
                    targetSchema,
                    table,
                    constraint
            );
            return count != null && count > 0 ? "MIGRATED" : "MISSING";
        } catch (DataAccessException ex) {
            return "ERROR: " + message(ex);
        }
    }

    private String objectStatus(String objectName, String objectType, String table) {
        if (blacklist.test(objectName)) {
            return "SKIPPED";
        }
        try (Connection connection = targetDataSource.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            boolean found = switch (objectType) {
                case "VIEW" -> hasTableType(metadata, connection, objectName, "VIEW");
                case "INDEX" -> hasIndex(metadata, connection, table, objectName);
                case "FOREIGN_KEY" -> hasForeignKey(metadata, connection, table, objectName);
                default -> false;
            };
            return found ? "MIGRATED" : "MISSING";
        } catch (SQLException ex) {
            return "ERROR: " + message(ex);
        }
    }

    private boolean hasTableType(DatabaseMetaData metadata, Connection connection, String object, String type)
            throws SQLException {
        try (ResultSet rs = metadata.getTables(connection.getCatalog(), targetSchema, object, new String[]{type})) {
            while (rs.next()) {
                if (object.equals(rs.getString("TABLE_NAME"))) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean hasIndex(DatabaseMetaData metadata, Connection connection, String table, String index)
            throws SQLException {
        try (ResultSet rs = metadata.getIndexInfo(connection.getCatalog(), targetSchema, table, false, false)) {
            while (rs.next()) {
                if (index.equals(rs.getString("INDEX_NAME"))) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean hasForeignKey(DatabaseMetaData metadata, Connection connection, String table, String key)
            throws SQLException {
        try (ResultSet rs = metadata.getImportedKeys(connection.getCatalog(), targetSchema, table)) {
            while (rs.next()) {
                if (key.equals(rs.getString("FK_NAME"))) {
                    return true;
                }
            }
            return false;
        }
    }

    private String renderSection(String title, String[] headers, List<String[]> inputRows) {
        List<String[]> rows = inputRows.isEmpty() ? Collections.singletonList(emptyRow(headers.length)) : inputRows;
        int[] widths = new int[headers.length];
        for (int column = 0; column < headers.length; column++) {
            widths[column] = headers[column].length();
            for (String[] row : rows) {
                widths[column] = Math.max(widths[column], row[column] == null ? 0 : row[column].length());
            }
        }

        String horizontal = horizontal(widths);
        StringBuilder out = new StringBuilder(title).append('\n').append(horizontal).append('\n')
                .append(row(headers, widths)).append('\n').append(horizontal).append('\n');
        rows.forEach(value -> out.append(row(value, widths)).append('\n'));
        return out.append(horizontal).append('\n').toString();
    }

    private String[] emptyRow(int length) {
        String[] row = new String[length];
        row[0] = "(none)";
        for (int i = 1; i < length; i++) row[i] = "-";
        return row;
    }

    private String horizontal(int[] widths) {
        StringBuilder out = new StringBuilder("+");
        for (int width : widths) out.append("-".repeat(width + 2)).append('+');
        return out.toString();
    }

    private String row(String[] values, int[] widths) {
        StringBuilder out = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String value = values[i] == null ? "" : values[i];
            out.append(' ').append(value).append(" ".repeat(widths[i] - value.length() + 1)).append('|');
        }
        return out.toString();
    }

    private String difference(BigDecimal difference) {
        int compared = difference.compareTo(BigDecimal.ZERO);
        if (compared == 0) return "MATCH";
        String value = difference.stripTrailingZeros().toPlainString();
        return compared > 0 ? "+" + value : value;
    }

    private String number(BigDecimal value) {
        if (value == null) return "-";
        if (value.compareTo(BigDecimal.ZERO) == 0) return "0";
        return value.stripTrailingZeros().toPlainString();
    }

    private String qualified(String schema, String object) {
        return quote(schema) + "." + quote(object);
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String message(Throwable throwable) {
        String value = throwable.getMessage();
        if ((value == null || value.isBlank()) && throwable.getCause() != null) {
            return message(throwable.getCause());
        }
        if (value == null || value.isBlank()) return throwable.toString();
        value = value.replaceAll("\\s+", " ").trim();
        return value.length() <= 80 ? value : value.substring(0, 80);
    }

    private record NumericResult(BigDecimal value, String error) {
        static NumericResult success(BigDecimal value) {
            return new NumericResult(value, null);
        }

        static NumericResult error(String error) {
            return new NumericResult(null, error);
        }

        boolean success() {
            return error == null;
        }

        String display() {
            if (!success()) return "ERR: " + error;
            if (value.compareTo(BigDecimal.ZERO) == 0) return "0";
            return value.stripTrailingZeros().toPlainString();
        }
    }
}
