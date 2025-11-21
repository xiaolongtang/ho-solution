package com.example.h2sync.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Statement;
import java.sql.Types;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OracleLoaderServiceTest {

    @Test
    void runFullRefreshUsesIndependentConnections() {
        String schema = "TEST";
        JdbcTemplate target = new JdbcTemplate(newH2DataSource("h2target" + randomSuffix()));

        DriverManagerDataSource oracleDelegate = newH2DataSource("oraclesrc" + randomSuffix());
        JdbcTemplate oracleJdbc = new JdbcTemplate(oracleDelegate);
        setupOracleSource(oracleJdbc, schema);

        TrackingDataSource trackingOracle = new TrackingDataSource(oracleDelegate, 25);

        OracleLoaderService loader = new OracleLoaderService(
                target,
                trackingOracle,
                schema,
                3,
                2,
                2,
                ""
        );

        loader.runFullRefresh();

        assertEquals(3, target.queryForObject("SELECT COUNT(*) FROM \"EMP\"", Integer.class));
        assertEquals(2, target.queryForObject("SELECT COUNT(*) FROM \"DEPT\"", Integer.class));
        assertEquals(3, target.queryForObject("SELECT COUNT(*) FROM \"VW_EMP_VIEW\"", Integer.class));

        assertEquals(0, trackingOracle.getOpenConnections(), "Oracle connections must be closed after refresh");
        assertTrue(trackingOracle.getMaxOpenConnections() >= 2,
                "Expected at least two concurrent Oracle connections during parallel load");
        assertEquals(7, trackingOracle.getTotalConnections(),
                "Unexpected number of Oracle connections opened during refresh");
    }

    @Test
    void mapTypeHandlesNegativeScaleForOracleNumber() throws Exception {
        OracleLoaderService loader = new OracleLoaderService(
                new JdbcTemplate(newH2DataSource("target" + randomSuffix())),
                newH2DataSource("oracle" + randomSuffix()),
                "TEST",
                1,
                100,
                1,
                ""
        );

        Method method = OracleLoaderService.class.getDeclaredMethod("mapType", int.class, int.class, int.class, int.class);
        method.setAccessible(true);

        String decimalType = (String) method.invoke(loader, Types.NUMERIC, 0, -127, 0);
        assertEquals("DECIMAL(38,12)", decimalType);

        String integerType = (String) method.invoke(loader, Types.NUMERIC, 5, 0, 0);
        assertEquals("INTEGER", integerType);

        String varcharType = (String) method.invoke(loader, Types.VARCHAR, 0, 0, 2000);
        assertEquals("VARCHAR(2000)", varcharType);
    }

    @Test
    void readColumnValueConvertsLobsToSupportedTypes() throws Exception {
        OracleLoaderService loader = new OracleLoaderService(
                new JdbcTemplate(newH2DataSource("target" + randomSuffix())),
                newH2DataSource("oracle" + randomSuffix()),
                "TEST",
                1,
                100,
                1,
                ""
        );

        byte[] blobData = new byte[]{0x01, 0x02, 0x03};

        ResultSet blobRs = mock(ResultSet.class);
        ResultSetMetaData blobMeta = mock(ResultSetMetaData.class);
        when(blobMeta.getColumnType(1)).thenReturn(Types.BLOB);
        when(blobRs.getBytes(1)).thenReturn(blobData);
        when(blobRs.wasNull()).thenReturn(false);
        Object blobValue = OracleJdbcValueConverter.readColumnValue(blobRs, blobMeta, 1, loader.log);
        assertArrayEquals(blobData, (byte[]) blobValue);

        ResultSet nullBlobRs = mock(ResultSet.class);
        ResultSetMetaData nullBlobMeta = mock(ResultSetMetaData.class);
        when(nullBlobMeta.getColumnType(1)).thenReturn(Types.BLOB);
        when(nullBlobRs.getBytes(1)).thenReturn(null);
        when(nullBlobRs.wasNull()).thenReturn(true);
        Object nullBlobValue = OracleJdbcValueConverter.readColumnValue(nullBlobRs, nullBlobMeta, 1, loader.log);
        assertNull(nullBlobValue);

        ResultSet blobObjectRs = mock(ResultSet.class);
        ResultSetMetaData blobObjectMeta = mock(ResultSetMetaData.class);
        when(blobObjectMeta.getColumnType(1)).thenReturn(Types.OTHER);
        SerialBlob serialBlob = new SerialBlob(blobData);
        when(blobObjectRs.getObject(1)).thenReturn(serialBlob);
        Object blobObjectValue = OracleJdbcValueConverter.readColumnValue(blobObjectRs, blobObjectMeta, 1, loader.log);
        assertArrayEquals(blobData, (byte[]) blobObjectValue);

        ResultSet clobRs = mock(ResultSet.class);
        ResultSetMetaData clobMeta = mock(ResultSetMetaData.class);
        when(clobMeta.getColumnType(1)).thenReturn(Types.CLOB);
        when(clobRs.getString(1)).thenReturn("hello");
        when(clobRs.wasNull()).thenReturn(false);
        Object clobValue = OracleJdbcValueConverter.readColumnValue(clobRs, clobMeta, 1, loader.log);
        assertEquals("hello", clobValue);

        ResultSet nullClobRs = mock(ResultSet.class);
        ResultSetMetaData nullClobMeta = mock(ResultSetMetaData.class);
        when(nullClobMeta.getColumnType(1)).thenReturn(Types.CLOB);
        when(nullClobRs.getString(1)).thenReturn(null);
        when(nullClobRs.wasNull()).thenReturn(true);
        Object nullClobValue = OracleJdbcValueConverter.readColumnValue(nullClobRs, nullClobMeta, 1, loader.log);
        assertNull(nullClobValue);

        ResultSet clobObjectRs = mock(ResultSet.class);
        ResultSetMetaData clobObjectMeta = mock(ResultSetMetaData.class);
        when(clobObjectMeta.getColumnType(1)).thenReturn(Types.OTHER);
        SerialClob serialClob = new SerialClob("world".toCharArray());
        when(clobObjectRs.getObject(1)).thenReturn(serialClob);
        Object clobObjectValue = OracleJdbcValueConverter.readColumnValue(clobObjectRs, clobObjectMeta, 1, loader.log);
        assertEquals("world", clobObjectValue);
    }

    @Test
    void readColumnValueConvertsTemporalTypesWhenGetTimestampFails() throws Exception {
        OracleLoaderService loader = new OracleLoaderService(
                new JdbcTemplate(newH2DataSource("target" + randomSuffix())),
                newH2DataSource("oracle" + randomSuffix()),
                "TEST",
                1,
                100,
                1,
                ""
        );

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(md.getColumnType(1)).thenReturn(Types.TIMESTAMP);
        when(rs.getTimestamp(1)).thenThrow(new SQLException("simulated"));
        when(rs.getObject(1)).thenReturn(java.time.LocalDateTime.of(2024, 2, 3, 4, 5, 6));

        Object converted = OracleJdbcValueConverter.readColumnValue(rs, md, 1, loader.log);

        assertEquals(Timestamp.valueOf("2024-02-03 04:05:06"), converted);
    }

    @Test
    void translateViewSqlUppercasesIdentifiersAndRewritesNvl2() throws Exception {
        OracleLoaderService loader = new OracleLoaderService(
                new JdbcTemplate(newH2DataSource("target" + randomSuffix())),
                newH2DataSource("oracle" + randomSuffix()),
                "TEST",
                1,
                100,
                1,
                ""
        );

        String oracleSql = "select emp.id, nvl2(emp.name, emp.name, 'n/a') name_copy, nvl2(emp.dept_id, emp.dept_id, 0) dept_id " +
                "from test.emp emp where nvl2(emp.status, emp.status, 'A') = 'A'";

        OracleViewSqlTranslator translator = new OracleViewSqlTranslator("TEST");
        String translated = translator.translate(oracleSql);

        String expected = "SELECT EMP.ID, CASE WHEN EMP.NAME IS NOT NULL THEN EMP.NAME ELSE 'n/a' END NAME_COPY, " +
                "CASE WHEN EMP.DEPT_ID IS NOT NULL THEN EMP.DEPT_ID ELSE 0 END DEPT_ID FROM EMP EMP WHERE " +
                "CASE WHEN EMP.STATUS IS NOT NULL THEN EMP.STATUS ELSE 'A' END = 'A'";

        assertEquals(expected, translated);
    }

    @Test
    void translateViewSqlQuotesH2ReservedKeywords() throws Exception {
        OracleLoaderService loader = new OracleLoaderService(
                new JdbcTemplate(newH2DataSource("target" + randomSuffix())),
                newH2DataSource("oracle" + randomSuffix()),
                "TEST",
                1,
                100,
                1,
                ""
        );

        String oracleSql = "select value, type, other.value value_alias from test.sample other " +
                "where type = 'A' and other.type > 0";

        OracleViewSqlTranslator translator = new OracleViewSqlTranslator("TEST");
        String translated = translator.translate(oracleSql);

        String expected = "SELECT \"VALUE\", \"TYPE\", OTHER.\"VALUE\" VALUE_ALIAS FROM SAMPLE OTHER " +
                "WHERE \"TYPE\" = 'A' AND OTHER.\"TYPE\" > 0";

        assertEquals(expected, translated);
    }

    private static DriverManagerDataSource newH2DataSource(String dbName) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:" + dbName + ";MODE=Oracle;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }

    private static String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase(Locale.ROOT);
    }

    private static void setupOracleSource(JdbcTemplate jdbc, String schema) {
        jdbc.execute("DROP TABLE IF EXISTS ALL_TABLES");
        jdbc.execute("DROP TABLE IF EXISTS ALL_VIEWS");
        jdbc.execute("DROP TABLE IF EXISTS ALL_SEQUENCES");
        jdbc.execute("CREATE TABLE ALL_TABLES (OWNER VARCHAR(128), TABLE_NAME VARCHAR(128))");
        jdbc.execute("CREATE TABLE ALL_VIEWS (OWNER VARCHAR(128), VIEW_NAME VARCHAR(128))");
        jdbc.execute("CREATE TABLE ALL_SEQUENCES (SEQUENCE_OWNER VARCHAR(128), SEQUENCE_NAME VARCHAR(128), " +
                "INCREMENT_BY BIGINT, LAST_NUMBER DECIMAL(38,0))");

        jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
        jdbc.execute("DROP TABLE IF EXISTS " + schema + ".EMP");
        jdbc.execute("CREATE TABLE " + schema + ".EMP (ID INT PRIMARY KEY, NAME VARCHAR(64), SALARY DECIMAL(10,2))");
        jdbc.update("INSERT INTO " + schema + ".EMP (ID, NAME, SALARY) VALUES (?,?,?)", 1, "Alice", BigDecimal.valueOf(100.10));
        jdbc.update("INSERT INTO " + schema + ".EMP (ID, NAME, SALARY) VALUES (?,?,?)", 2, "Bob", BigDecimal.valueOf(150.25));
        jdbc.update("INSERT INTO " + schema + ".EMP (ID, NAME, SALARY) VALUES (?,?,?)", 3, "Carol", BigDecimal.valueOf(200.50));

        jdbc.execute("DROP TABLE IF EXISTS " + schema + ".DEPT");
        jdbc.execute("CREATE TABLE " + schema + ".DEPT (ID INT PRIMARY KEY, TITLE VARCHAR(64))");
        jdbc.update("INSERT INTO " + schema + ".DEPT (ID, TITLE) VALUES (?,?)", 10, "Sales");
        jdbc.update("INSERT INTO " + schema + ".DEPT (ID, TITLE) VALUES (?,?)", 20, "Engineering");

        jdbc.execute("DROP VIEW IF EXISTS " + schema + ".EMP_VIEW");
        jdbc.execute("CREATE VIEW " + schema + ".EMP_VIEW AS SELECT ID, NAME FROM " + schema + ".EMP");

        jdbc.update("INSERT INTO ALL_TABLES (OWNER, TABLE_NAME) VALUES (?, ?)", schema, "EMP");
        jdbc.update("INSERT INTO ALL_TABLES (OWNER, TABLE_NAME) VALUES (?, ?)", schema, "DEPT");
        jdbc.update("INSERT INTO ALL_VIEWS (OWNER, VIEW_NAME) VALUES (?, ?)", schema, "EMP_VIEW");
    }

    static final class TrackingDataSource implements DataSource {
        private final DataSource delegate;
        private final long queryDelayMs;
        private final AtomicInteger openConnections = new AtomicInteger();
        private final AtomicInteger maxOpenConnections = new AtomicInteger();
        private final AtomicInteger totalConnections = new AtomicInteger();
        private final Set<Integer> connectionIds = ConcurrentHashMap.newKeySet();

        TrackingDataSource(DataSource delegate, long queryDelayMs) {
            this.delegate = delegate;
            this.queryDelayMs = queryDelayMs;
        }

        int getOpenConnections() {
            return openConnections.get();
        }

        int getMaxOpenConnections() {
            return maxOpenConnections.get();
        }

        int getTotalConnections() {
            return totalConnections.get();
        }

        int getUniqueConnectionCount() {
            return connectionIds.size();
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection real = delegate.getConnection();
            return wrapAndTrack(real);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            Connection real = delegate.getConnection(username, password);
            return wrapAndTrack(real);
        }

        private Connection wrapAndTrack(Connection real) {
            connectionIds.add(System.identityHashCode(real));
            int open = openConnections.incrementAndGet();
            maxOpenConnections.updateAndGet(current -> Math.max(current, open));
            totalConnections.incrementAndGet();
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class[]{Connection.class},
                    new TrackingConnectionHandler(real)
            );
        }

        private class TrackingConnectionHandler implements InvocationHandler {
            private final Connection delegate;
            private final AtomicBoolean closed = new AtomicBoolean(false);

            TrackingConnectionHandler(Connection delegate) {
                this.delegate = delegate;
            }

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                switch (name) {
                    case "close":
                        if (closed.compareAndSet(false, true)) {
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            } finally {
                                openConnections.decrementAndGet();
                            }
                        }
                        return null;
                    case "prepareStatement":
                        return wrapStatement(method, delegate, args);
                    case "createStatement":
                        return wrapStatement(method, delegate, args);
                    case "unwrap":
                        Class<?> target = (Class<?>) args[0];
                        if (target.isInstance(proxy)) {
                            return proxy;
                        }
                        if (target.isInstance(delegate)) {
                            return delegate;
                        }
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    case "isWrapperFor":
                        Class<?> iface = (Class<?>) args[0];
                        if (iface.isInstance(proxy) || iface.isInstance(delegate)) {
                            return true;
                        }
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return "TrackingConnection[" + delegate + "]";
                    default:
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                }
            }
        }

        private Object wrapStatement(Method method, Connection delegate, Object[] args) throws Throwable {
            Object stmt;
            try {
                stmt = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            Class<?>[] ifaces = stmt instanceof PreparedStatement
                    ? new Class[]{PreparedStatement.class}
                    : new Class[]{Statement.class};
            return Proxy.newProxyInstance(
                    stmt.getClass().getClassLoader(),
                    ifaces,
                    new TrackingStatementHandler(stmt)
            );
        }

        private class TrackingStatementHandler implements InvocationHandler {
            private final Object delegate;
            private final AtomicBoolean closed = new AtomicBoolean(false);

            TrackingStatementHandler(Object delegate) {
                this.delegate = delegate;
            }

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                if ("close".equals(name)) {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    }
                    return null;
                }
                if ("executeQuery".equals(name) || "execute".equals(name)) {
                    delay();
                }
                if ("unwrap".equals(name)) {
                    Class<?> target = (Class<?>) args[0];
                    if (target.isInstance(proxy)) {
                        return proxy;
                    }
                    if (target.isInstance(delegate)) {
                        return delegate;
                    }
                }
                if ("isWrapperFor".equals(name)) {
                    Class<?> iface = (Class<?>) args[0];
                    if (iface.isInstance(proxy) || iface.isInstance(delegate)) {
                        return true;
                    }
                }
                if ("equals".equals(name)) {
                    return proxy == args[0];
                }
                if ("hashCode".equals(name)) {
                    return System.identityHashCode(proxy);
                }
                if ("toString".equals(name)) {
                    return "TrackingStatement[" + delegate + "]";
                }
                try {
                    return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        }

        private void delay() {
            if (queryDelayMs <= 0) {
                return;
            }
            try {
                Thread.sleep(queryDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }

        @Override
        public java.io.PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(java.io.PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getGlobal();
        }
    }
}
