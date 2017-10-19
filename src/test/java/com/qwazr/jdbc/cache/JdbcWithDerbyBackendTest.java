/**
 * Copyright 2016 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.jdbc.cache;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.qwazr.jdbc.cache.DbTestUtil.ROW1;
import static com.qwazr.jdbc.cache.DbTestUtil.ROW2;
import static com.qwazr.jdbc.cache.DbTestUtil.ROW4;
import static com.qwazr.jdbc.cache.DbTestUtil.ROWS;
import static com.qwazr.jdbc.cache.DbTestUtil.checkNotSupported;
import static com.qwazr.jdbc.cache.DbTestUtil.checkResultSet;
import static com.qwazr.jdbc.cache.DbTestUtil.executeQuery;
import static com.qwazr.jdbc.cache.DbTestUtil.getCallableStatement;
import static com.qwazr.jdbc.cache.DbTestUtil.getPreparedStatement;
import static com.qwazr.jdbc.cache.DbTestUtil.updateGetResultSet;

abstract public class JdbcWithDerbyBackendTest extends JdbcTest {

    private static boolean dbCreated;

    abstract String getDerbyDbName();
    abstract Class<? extends ResultSet> expectedResultSetClass();
    abstract boolean isCacheEnabled();

    @BeforeClass
    public static void init() throws ClassNotFoundException, IOException {
        dbCreated = false;
    }

    /**
     * We make sure before starting a test that we have all the connections available
     * and that the database has been initialized
     * @throws SQLException in case we can't execute things on the database
     */
    @Before
    public void createConnectionsAndSampleDb() throws SQLException {
        if (!dbCreated) {
            // Init the backend database so we can run our tests on it
            dbCreated = DbTestUtil.initTestDb(getDerbyDbName());
        }
    }

    @Test
    public void test110TestSimpleStatement() throws SQLException, IOException {
        // First the cache might be written
        checkResultSet(checkCache(executeQuery(getConnection())), ROWS);
        // Second the cache might be read
        checkResultSet(checkCache(executeQuery(getConnection())), ROWS);
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled()) {
            checkResultSet(checkCache(executeQuery(getNoBackendConnection())), ROWS);
        }
    }

    @Test
    public void test110TestUpdateAndGetResultSet() throws SQLException, IOException {
        checkResultSet(checkCache(updateGetResultSet(getConnection())), ROWS);
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled()) {
            checkResultSet(checkCache(updateGetResultSet(getConnection())), ROWS);
        }
    }

    @Test
    public void test120TestPreparedStatement() throws SQLException, IOException {
        final PreparedStatement stmt = getPreparedStatement(getConnection(), ROW1, ROW4);
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled()) {
            checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
        }
    }

    @Test
    public void test120TestCallableStatement() throws SQLException, IOException {
        final CallableStatement stmt = getCallableStatement(getConnection());
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), ROW2);
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), ROW2);
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled()) {
            checkResultSet(checkCache(stmt.executeQuery()), ROW2);
        }
    }

    @Test
    public void test135TestCallableStatementGetResultSet() throws SQLException, IOException {
        final CallableStatement stmt = getCallableStatement(getConnection());
        stmt.execute();
        checkResultSet(checkCache(stmt.getResultSet()), ROW2);
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled()) {
            checkResultSet(checkCache(stmt.getResultSet()), ROW2);
        }
    }

    @Test
    public void test500ThreadSafeTest() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        final long exitTime = System.currentTimeMillis() + 10 * 1000;
        final AtomicInteger count = new AtomicInteger();
        try {
            for (int i = 0; i < 8; i++) {
                executor.submit(() -> {
                    while (System.currentTimeMillis() < exitTime)
                        try {
                            test120TestPreparedStatement();
                            count.incrementAndGet();
                        } catch (SQLException | IOException e) {
                            throw new RuntimeException(e);
                        }
                });
                executor.submit(() -> {
                    while (System.currentTimeMillis() < exitTime)
                        try {
                            test120TestCallableStatement();
                            count.incrementAndGet();
                        } catch (SQLException | IOException e) {
                            throw new RuntimeException(e);
                        }
                });
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
            LOGGER.info("Iteration count: " + count.get());
        } finally {
            if (!executor.isShutdown())
                executor.shutdownNow();
        }
    }

    @Test
    public void test600TestResultSetMetaData() throws SQLException {
        final ResultSetMetaData metaData = getPreparedStatement(getConnection(), ROW1, ROW4).executeQuery()
                .getMetaData();
        Assert.assertNotNull(metaData);
        int colCount = metaData.getColumnCount();
        Assert.assertTrue(colCount > 0);
        for (int i = 1; i <= colCount; i++) {
            Assert.assertNotNull(metaData.getColumnName(i));
            Assert.assertNotNull(metaData.getColumnLabel(i));
            Assert.assertNotNull(metaData.getColumnTypeName(i));
            Assert.assertNotNull(metaData.getCatalogName(i));
            Assert.assertNotNull(metaData.getColumnClassName(i));
            Assert.assertTrue(metaData.getColumnDisplaySize(i) > 0);
            Assert.assertFalse(metaData.isReadOnly(i));
            Assert.assertNotNull(metaData.getSchemaName(i));
            Assert.assertNotNull(metaData.getTableName(i));
            Assert.assertTrue(metaData.getPrecision(i) > 0);
            Assert.assertTrue(metaData.isSearchable(i));
            if (i == 1) {
                Assert.assertTrue(metaData.isSigned(i));
                Assert.assertEquals(0, metaData.getScale(i));
                Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(i));
                Assert.assertFalse(metaData.isCaseSensitive(i));
                Assert.assertFalse(metaData.isAutoIncrement(i));
                Assert.assertFalse(metaData.isCurrency(i));
                Assert.assertFalse(metaData.isWritable(i));
            } else {
                Assert.assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(i));
            }
        }
    }

    @Test
    public void test800ResultSetNotSupportedMethod() throws SQLException {
        ResultSet resultSet = getPreparedStatement(getConnection(), ROW1, ROW4).executeQuery();
        checkNotSupported(() -> resultSet.updateArray(1, null));
        checkNotSupported(() -> resultSet.updateArray("id", null));
        checkNotSupported(() -> resultSet.updateAsciiStream(1, null));
        checkNotSupported(() -> resultSet.updateAsciiStream("id", null));
        checkNotSupported(() -> resultSet.updateBigDecimal(1, null));
        checkNotSupported(() -> resultSet.updateBigDecimal("id", null));
        checkNotSupported(() -> resultSet.updateByte(1, (byte) 0));
        checkNotSupported(() -> resultSet.updateByte("id", (byte) 0));
        checkNotSupported(() -> resultSet.updateBytes(1, null));
        checkNotSupported(() -> resultSet.updateBytes("id", null));
        checkNotSupported(() -> resultSet.updateBoolean(1, false));
        checkNotSupported(() -> resultSet.updateBoolean("id", false));
        checkNotSupported(() -> resultSet.updateDate(1, null));
        checkNotSupported(() -> resultSet.updateDate("id", null));
        checkNotSupported(() -> resultSet.updateDouble(1, 0d));
        checkNotSupported(() -> resultSet.updateDouble("id", 0d));
        checkNotSupported(() -> resultSet.updateFloat(1, 0f));
        checkNotSupported(() -> resultSet.updateFloat("id", 0f));
        checkNotSupported(() -> resultSet.updateInt(1, 0));
        checkNotSupported(() -> resultSet.updateInt("id", 0));
        checkNotSupported(() -> resultSet.updateLong(1, 0));
        checkNotSupported(() -> resultSet.updateLong("id", 0));
        checkNotSupported(() -> resultSet.updateRowId(1, null));
        checkNotSupported(() -> resultSet.updateRowId("id", null));
        checkNotSupported(() -> resultSet.updateNull(1));
        checkNotSupported(() -> resultSet.updateNull("id"));
        checkNotSupported(() -> resultSet.updateObject(1, null));
        checkNotSupported(() -> resultSet.updateObject("id", null));
    }

    @Test
    public void test900TestCacheAPI() throws SQLException, IOException {
        ResultSetCache cache = com.qwazr.jdbc.cache.Driver.getCache(getConnection());

        // We can only test the cache API in the context of a cacheable connection
        Assume.assumeNotNull(cache);

        // Empty the cache if anything has been ran before
        cache.flush();
        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(0, cache.active());

        executeQuery(getConnection());
        getPreparedStatement(getConnection(), ROW1, ROW4).executeQuery();
        getCallableStatement(getConnection()).executeQuery();

        Assert.assertEquals(3, cache.size());
        Assert.assertEquals(0, cache.active());

        Statement stmt = getPreparedStatement(getConnection(), ROW1, ROW4);
        Assert.assertTrue(cache.exists(stmt));

        Assert.assertFalse(cache.active(stmt));

        cache.flush(stmt);
        Assert.assertEquals(2, cache.size());

        cache.flush();
        Assert.assertEquals(0, cache.size());
    }

    private ResultSet checkCache(ResultSet resultSet) {
        Assert.assertEquals(expectedResultSetClass().getName(), resultSet.getClass().getName());
        return resultSet;
    }
}
