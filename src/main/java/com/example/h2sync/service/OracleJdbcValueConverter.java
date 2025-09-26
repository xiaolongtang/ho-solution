package com.example.h2sync.service;

import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * Utility methods for converting Oracle JDBC column values into H2-compatible representations.
 */
final class OracleJdbcValueConverter {

    private OracleJdbcValueConverter() {
    }

    static Object readColumnValue(ResultSet rs, ResultSetMetaData md, int index, Logger log) throws SQLException {
        int jdbcType = md.getColumnType(index);

        if (isBinaryType(jdbcType)) {
            byte[] data = rs.getBytes(index);
            return rs.wasNull() ? null : data;
        }

        if (isClobType(jdbcType)) {
            String text = rs.getString(index);
            return rs.wasNull() ? null : text;
        }

        if (isTemporalType(jdbcType)) {
            try {
                Timestamp ts = rs.getTimestamp(index);
                if (ts != null || rs.wasNull()) {
                    return ts;
                }
            } catch (SQLException ex) {
                log.debug("Oracle timestamp conversion failed for column {} using getTimestamp: {}", index, ex.getMessage());
            }
        }

        Object value = rs.getObject(index);

        if (value instanceof Blob) {
            return blobToBytes((Blob) value);
        }
        if (value instanceof Clob) {
            return clobToString((Clob) value);
        }

        if (isTemporalType(jdbcType)) {
            return toTimestamp(value);
        }
        return maybeConvertTemporal(value);
    }

    private static boolean isTemporalType(int jdbcType) {
        return jdbcType == Types.DATE
                || jdbcType == Types.TIME
                || jdbcType == Types.TIMESTAMP
                || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE
                || jdbcType == Types.TIME_WITH_TIMEZONE;
    }

    private static boolean isBinaryType(int jdbcType) {
        return jdbcType == Types.BLOB
                || jdbcType == Types.BINARY
                || jdbcType == Types.VARBINARY
                || jdbcType == Types.LONGVARBINARY;
    }

    private static boolean isClobType(int jdbcType) {
        return jdbcType == Types.CLOB
                || jdbcType == Types.NCLOB
                || jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.LONGNVARCHAR;
    }

    private static Object maybeConvertTemporal(Object value) throws SQLException {
        if (value == null) return null;
        if (value instanceof Timestamp) return value;
        if (value instanceof java.sql.Date || value instanceof java.sql.Time || value instanceof java.util.Date) {
            return toTimestamp(value);
        }
        if (value instanceof LocalDateTime || value instanceof LocalDate
                || value instanceof Instant || value instanceof OffsetDateTime
                || value instanceof ZonedDateTime) {
            return toTimestamp(value);
        }
        String className = value.getClass().getName();
        if (className.startsWith("oracle.sql.")) {
            return toTimestamp(value);
        }
        return value;
    }

    private static byte[] blobToBytes(Blob blob) throws SQLException {
        if (blob == null) {
            return null;
        }
        try {
            long length = blob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException("BLOB length exceeds supported size: " + length);
            }
            return blob.getBytes(1, (int) length);
        } finally {
            free(blob);
        }
    }

    private static String clobToString(Clob clob) throws SQLException {
        if (clob == null) {
            return null;
        }
        try {
            long length = clob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException("CLOB length exceeds supported size: " + length);
            }
            return clob.getSubString(1, (int) length);
        } finally {
            free(clob);
        }
    }

    private static void free(Blob blob) {
        if (blob == null) {
            return;
        }
        try {
            blob.free();
        } catch (AbstractMethodError | SQLException ignored) {
            // Some drivers (or older JDBC versions) do not support free(); ignore such cases.
        }
    }

    private static void free(Clob clob) {
        if (clob == null) {
            return;
        }
        try {
            clob.free();
        } catch (AbstractMethodError | SQLException ignored) {
            // Ignore drivers that do not implement free()
        }
    }

    private static Object toTimestamp(Object value) throws SQLException {
        if (value == null) return null;
        if (value instanceof Timestamp) return value;
        if (value instanceof java.sql.Date) {
            return new Timestamp(((java.sql.Date) value).getTime());
        }
        if (value instanceof java.sql.Time) {
            return new Timestamp(((java.sql.Time) value).getTime());
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return Timestamp.valueOf(((LocalDate) value).atStartOfDay());
        }
        if (value instanceof Instant) {
            return Timestamp.from((Instant) value);
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.valueOf(((OffsetDateTime) value).toLocalDateTime());
        }
        if (value instanceof ZonedDateTime) {
            return Timestamp.valueOf(((ZonedDateTime) value).toLocalDateTime());
        }

        String className = value.getClass().getName();
        if (className.startsWith("oracle.sql.")) {
            try {
                Method method = value.getClass().getMethod("timestampValue");
                Object ts = method.invoke(value);
                if (ts instanceof Timestamp) {
                    return ts;
                }
            } catch (NoSuchMethodException ignored) {
                // fall through and try other conversion methods
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new SQLException("Failed to convert Oracle temporal value of type " + className, e);
            }

            try {
                Method method = value.getClass().getMethod("dateValue");
                Object date = method.invoke(value);
                if (date instanceof java.sql.Date) {
                    return new Timestamp(((java.sql.Date) date).getTime());
                }
            } catch (NoSuchMethodException ignored) {
                // fall through
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new SQLException("Failed to convert Oracle temporal value of type " + className, e);
            }

            try {
                Method method = value.getClass().getMethod("timeValue");
                Object time = method.invoke(value);
                if (time instanceof java.sql.Time) {
                    return new Timestamp(((java.sql.Time) time).getTime());
                }
            } catch (NoSuchMethodException ignored) {
                // fall through
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new SQLException("Failed to convert Oracle temporal value of type " + className, e);
            }
        }

        return value;
    }
}
