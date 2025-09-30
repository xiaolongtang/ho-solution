package com.example.h2sync.service;

import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

class MigrationReportPrinter {

    private static final List<String> H2_SEQUENCE_VALUE_COLUMNS = List.of(
            "CURRENT_VALUE",
            "VALUE",
            "LAST_VALUE",
            "BASE_VALUE",
            "START_VALUE",
            "START_WITH"
    );

    private final Logger log;
    private final JdbcTemplate h2;
    private final DataSource oracleDs;
    private final UnaryOperator<String> identifierQuoter;
    private final Predicate<String> blacklistPredicate;
    private final String oracleSchema;

    MigrationReportPrinter(
            Logger log,
            JdbcTemplate h2,
            DataSource oracleDs,
            UnaryOperator<String> identifierQuoter,
            Predicate<String> blacklistPredicate,
            String oracleSchema
    ) {
        this.log = Objects.requireNonNull(log, "log");
        this.h2 = Objects.requireNonNull(h2, "h2");
        this.oracleDs = Objects.requireNonNull(oracleDs, "oracleDs");
        this.identifierQuoter = Objects.requireNonNull(identifierQuoter, "identifierQuoter");
        this.blacklistPredicate = Objects.requireNonNull(blacklistPredicate, "blacklistPredicate");
        this.oracleSchema = oracleSchema;
    }

    void printReport(Set<String> tables, Set<String> views, List<Map<String, Object>> sequences) {
        List<String[]> tableRows = new ArrayList<>();
        for (String table : tables) {
            tableRows.add(buildTableRow(table));
        }

        List<String[]> viewRows = new ArrayList<>();
        for (String view : views) {
            viewRows.add(buildViewRow(view));
        }

        List<String[]> sequenceRows = new ArrayList<>();
        sequences.stream()
                .sorted(Comparator.comparing(seq -> {
                    String name = (String) seq.get("SEQUENCE_NAME");
                    return name == null ? "" : name.toUpperCase(Locale.ROOT);
                }))
                .forEach(sequence -> sequenceRows.add(buildSequenceRow(sequence)));

        StringBuilder sb = new StringBuilder();
        sb.append("==================== MIGRATION REPORT ====================\n\n");
        sb.append(renderSection("Tables", new String[]{"Table", "Oracle Rows", "H2 Rows", "Status"}, tableRows)).append('\n');
        sb.append(renderSection("Views", new String[]{"View", "Status"}, viewRows)).append('\n');
        sb.append(renderSection("Sequences", new String[]{"Sequence", "Oracle Max", "H2 Max", "Status"}, sequenceRows));
        sb.append("==========================================================");

        log.info("\n{}", sb);
    }

    private String[] buildTableRow(String table) {
        if (isBlacklisted(table)) {
            return new String[]{table, "-", "-", "SKIPPED"};
        }
        NumericResult oracle = fetchOracleTableCount(table);
        NumericResult h2Result = fetchH2TableCount(table);
        String status;
        if (!oracle.isSuccess() || !h2Result.isSuccess()) {
            status = "ERROR";
        } else {
            status = formatDifference(h2Result.getValue().subtract(oracle.getValue()));
        }
        return new String[]{table, oracle.display(), h2Result.display(), status};
    }

    private String[] buildViewRow(String view) {
        if (isBlacklisted(view)) {
            return new String[]{view, "SKIPPED"};
        }
        PresenceResult presence = checkH2View(view);
        if (presence.hasError()) {
            return new String[]{view, "ERROR: " + presence.getError()};
        }
        return new String[]{view, presence.isPresent() ? "MIGRATED" : "MISSING"};
    }

    private String[] buildSequenceRow(Map<String, Object> sequence) {
        String rawName = (String) sequence.get("SEQUENCE_NAME");
        String name = rawName == null ? "(UNKNOWN)" : rawName.toUpperCase(Locale.ROOT);
        if (rawName == null) {
            BigDecimal oracleValue = toBigDecimal(sequence.get("LAST_NUMBER"));
            return new String[]{name, formatNumber(oracleValue), "ERR: MISSING NAME", "ERROR"};
        }
        if (isBlacklisted(name)) {
            return new String[]{name, "-", "-", "SKIPPED"};
        }
        BigDecimal oracleValue = toBigDecimal(sequence.get("LAST_NUMBER"));
        NumericResult h2Value = fetchH2SequenceValue(name);
        String oracleDisplay = formatNumber(oracleValue);
        String status;
        if (!h2Value.isSuccess()) {
            status = "ERROR";
        } else {
            status = formatDifference(h2Value.getValue().subtract(oracleValue));
        }
        return new String[]{name, oracleDisplay, h2Value.display(), status};
    }

    private NumericResult fetchOracleTableCount(String table) {
        String qualified = oracleSchema == null ? table : oracleSchema + "." + table;
        String sql = "SELECT COUNT(1) FROM " + qualified;
        try (Connection conn = oracleDs.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                long count = rs.getLong(1);
                return NumericResult.success(BigDecimal.valueOf(count));
            }
            return NumericResult.error("NO DATA");
        } catch (SQLException ex) {
            return NumericResult.error(normalizeMessage(truncate(extractMessage(ex), 60)));
        }
    }

    private NumericResult fetchH2TableCount(String table) {
        String sql = "SELECT COUNT(1) FROM " + identifierQuoter.apply(table);
        try {
            Long count = h2.queryForObject(sql, Long.class);
            if (count == null) {
                return NumericResult.error("NO DATA");
            }
            return NumericResult.success(BigDecimal.valueOf(count));
        } catch (DataAccessException ex) {
            return NumericResult.error(normalizeMessage(truncate(extractMessage(ex), 60)));
        }
    }

    private PresenceResult checkH2View(String view) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(TABLE_NAME) = ? AND TABLE_SCHEMA = SCHEMA()";
        try {
            Integer count = h2.queryForObject(sql, Integer.class, view.toUpperCase(Locale.ROOT));
            return PresenceResult.success(count != null && count > 0);
        } catch (DataAccessException ex) {
            return PresenceResult.error(normalizeMessage(truncate(extractMessage(ex), 60)));
        }
    }

    private NumericResult fetchH2SequenceValue(String sequence) {
        String sql = "SELECT * FROM INFORMATION_SCHEMA.SEQUENCES " +
                "WHERE UPPER(SEQUENCE_NAME) = ? AND UPPER(SEQUENCE_SCHEMA) = UPPER(SCHEMA())";
        try {
            Map<String, Object> row = h2.queryForMap(sql, sequence.toUpperCase(Locale.ROOT));
            BigDecimal value = extractH2SequenceValue(row);
            if (value == null) {
                return NumericResult.error("VALUE NOT AVAILABLE");
            }
            return NumericResult.success(value);
        } catch (EmptyResultDataAccessException ex) {
            return NumericResult.error("NOT FOUND");
        } catch (DataAccessException ex) {
            return NumericResult.error(normalizeMessage(truncate(extractMessage(ex), 60)));
        }
    }

    private BigDecimal extractH2SequenceValue(Map<String, Object> row) {
        for (String column : H2_SEQUENCE_VALUE_COLUMNS) {
            if (!row.containsKey(column)) {
                continue;
            }
            Object value = row.get(column);
            if (value == null) {
                continue;
            }
            return toBigDecimal(value);
        }
        return null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        if (value == null) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException ex) {
            log.warn("Unable to convert value {} to BigDecimal", value);
            return BigDecimal.ZERO;
        }
    }

    private String renderSection(String title, String[] headers, List<String[]> rows) {
        List<String[]> dataRows = rows.isEmpty() ? Collections.singletonList(emptyRow(headers.length)) : rows;
        int columns = headers.length;
        int[] widths = new int[columns];
        for (int i = 0; i < columns; i++) {
            widths[i] = headers[i] != null ? headers[i].length() : 0;
        }
        for (String[] row : dataRows) {
            for (int i = 0; i < columns; i++) {
                String cell = row[i] == null ? "" : row[i];
                if (cell.length() > widths[i]) {
                    widths[i] = cell.length();
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(title).append('\n');
        String horizontal = buildHorizontal(widths);
        sb.append(horizontal).append('\n');
        sb.append(formatRow(headers, widths)).append('\n');
        sb.append(horizontal).append('\n');
        for (String[] row : dataRows) {
            sb.append(formatRow(row, widths)).append('\n');
        }
        sb.append(horizontal).append("\n");
        return sb.toString();
    }

    private String[] emptyRow(int length) {
        String[] row = new String[length];
        row[0] = "(none)";
        for (int i = 1; i < length; i++) {
            row[i] = "-";
        }
        return row;
    }

    private String buildHorizontal(int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append('+');
        }
        return sb.toString();
    }

    private String formatRow(String[] row, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < widths.length; i++) {
            String cell = row[i] == null ? "" : row[i];
            sb.append(' ').append(padRight(cell, widths[i])).append(' ').append('|');
        }
        return sb.toString();
    }

    private String padRight(String text, int width) {
        if (text.length() >= width) {
            return text;
        }
        return text + " ".repeat(width - text.length());
    }

    private String formatNumber(BigDecimal value) {
        if (value == null) {
            return "-";
        }
        BigDecimal normalized = value.stripTrailingZeros();
        if (normalized.compareTo(BigDecimal.ZERO) == 0) {
            return "0";
        }
        return normalized.toPlainString();
    }

    private String formatDifference(BigDecimal diff) {
        if (diff == null) {
            return "ERROR";
        }
        int comparison = diff.compareTo(BigDecimal.ZERO);
        if (comparison == 0) {
            return "MATCH";
        }
        BigDecimal normalized = diff.stripTrailingZeros();
        String plain = normalized.toPlainString();
        return comparison > 0 ? "+" + plain : plain;
    }

    private String normalizeMessage(String message) {
        if (message == null) {
            return "";
        }
        String cleaned = message.replaceAll("\s+", " ").trim();
        return cleaned.isEmpty() ? "UNKNOWN" : cleaned;
    }

    private String truncate(String s, int max) {
        if (s == null || s.length() <= max) {
            return s;
        }
        return s.substring(0, max);
    }

    private String extractMessage(Throwable ex) {
        if (ex == null) {
            return "";
        }
        String message = ex.getMessage();
        if (message != null && !message.isBlank()) {
            return message;
        }
        Throwable cause = ex.getCause();
        return cause == null ? ex.toString() : extractMessage(cause);
    }

    private boolean isBlacklisted(String name) {
        return blacklistPredicate.test(name);
    }

    private static final class NumericResult {
        private final BigDecimal value;
        private final String error;

        private NumericResult(BigDecimal value, String error) {
            this.value = value;
            this.error = error;
        }

        static NumericResult success(BigDecimal value) {
            return new NumericResult(value, null);
        }

        static NumericResult error(String message) {
            return new NumericResult(null, message);
        }

        boolean isSuccess() {
            return error == null;
        }

        BigDecimal getValue() {
            return value;
        }

        String display() {
            if (isSuccess()) {
                return formatStaticNumber(value);
            }
            return "ERR: " + error;
        }

        private static String formatStaticNumber(BigDecimal value) {
            if (value == null) {
                return "-";
            }
            BigDecimal normalized = value.stripTrailingZeros();
            if (normalized.compareTo(BigDecimal.ZERO) == 0) {
                return "0";
            }
            return normalized.toPlainString();
        }
    }

    private static final class PresenceResult {
        private final boolean present;
        private final String error;

        private PresenceResult(boolean present, String error) {
            this.present = present;
            this.error = error;
        }

        static PresenceResult success(boolean present) {
            return new PresenceResult(present, null);
        }

        static PresenceResult error(String message) {
            return new PresenceResult(false, message);
        }

        boolean isPresent() {
            return present;
        }

        boolean hasError() {
            return error != null;
        }

        String getError() {
            return error;
        }
    }
}

