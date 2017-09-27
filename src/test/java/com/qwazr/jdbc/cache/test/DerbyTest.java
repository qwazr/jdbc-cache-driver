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
package com.qwazr.jdbc.cache.test;

import com.qwazr.jdbc.cache.ResultSetCache;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DerbyTest {

    final static Logger LOGGER = Logger.getLogger(DerbyTest.class.getName());

    private static Connection cnxCacheDisable;
    private static Connection cnxCacheEnable;
    private static Connection cnxCacheNoBackend;

    private static String jdbcCacheUrl;

    @BeforeClass
    public static void init() throws ClassNotFoundException, IOException {
        Class.forName("com.qwazr.jdbc.cache.Driver");
        String tempDirPath = Files.createTempDirectory("jdbc-cache-test").toUri().getPath();
        if (tempDirPath.contains(":") && tempDirPath.startsWith("/"))
            tempDirPath = tempDirPath.substring(1);
        jdbcCacheUrl = "jdbc:cache:file:" + tempDirPath + File.separatorChar + "cache";
    }

    @Test
    public void test000testDriver() throws SQLException {
        Driver driver = DriverManager.getDriver(jdbcCacheUrl);
        Assert.assertNotNull(driver);
        Assert.assertEquals(1, driver.getMajorVersion());
        Assert.assertEquals(3, driver.getMinorVersion());
        Assert.assertNotNull(driver.getParentLogger());
        Assert.assertFalse(driver.jdbcCompliant());
        DriverPropertyInfo[] infos = driver.getPropertyInfo(null, null);
        Assert.assertNotNull(infos);
        Assert.assertEquals(3, infos.length);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_URL, infos[0].name);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_CLASS, infos[1].name);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_ACTIVE, infos[2].name);
    }

    @Test
    public void test001initConnectionWithoutCache() throws SQLException, IOException, ClassNotFoundException {
        final Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
        info.setProperty("cache.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
        info.setProperty("cache.driver.active", "false");
        cnxCacheDisable = DriverManager.getConnection(jdbcCacheUrl, info);
        Assert.assertNotNull(cnxCacheDisable);
    }

    @Test
    public void test002initConnectionWithCache() throws SQLException, IOException, ClassNotFoundException {
        final Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
        cnxCacheEnable = DriverManager.getConnection(jdbcCacheUrl, info);
        Assert.assertNotNull(cnxCacheEnable);
    }

    @Test
    public void test003initConnectionNoBackend() throws SQLException, IOException, ClassNotFoundException {
        cnxCacheNoBackend = DriverManager.getConnection(jdbcCacheUrl);
        Assert.assertNotNull(cnxCacheNoBackend);
    }

    final static Object[] ROW1 = { 10, "TEN", null, null, null, null, null, 1.11D, 1.1F, (short) 1, 1000000L, "clob1" };
    final static Object[] ROW2 = { 20, "TWENTY", null, null, null, null, null, 2.22D, 2.2F, (short) 2, 2000000L,
            "clob2" };
    final static Object[] ROW3 = { 30, "THIRTY", null, null, null, null, null, 3.33D, 3.3F, (short) 3, 3000000L,
            "clob3" };
    final static Object[] ROW4 = { 40, null, null, null, null, null, null, 4.44D, 4.4F, (short) 4, 4000000L, "clob4" };
    final static Object[] ROW5 = { 50, null, null, null, null, null, null, 5.55D, 5.5F, (short) 5, 5000000L, null };

    final static Object[][] ROWS = { ROW1, ROW2, ROW3, ROW4, ROW5 };

    final static String[] COLUMNS = { "ID", "NAME", "TS", "DT1", "DT2", "TI1", "TI2", "DBL", "FL", "TI", "BI", "CL" };

    final static String SQL_TABLE = "CREATE TABLE FIRSTTABLE (ID INT PRIMARY KEY, NAME VARCHAR(12), TS TIMESTAMP, "
            + "DT1 DATE, DT2 DATE, TI1 TIME, TI2 TIME, DBL DOUBLE, FL FLOAT, TI SMALLINT, BI BIGINT, CL CLOB)";
    final static String SQL_INSERT = "INSERT INTO FIRSTTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

    @Test
    public void test100createTableAndDataSet() throws SQLException, InterruptedException {
        cnxCacheDisable.createStatement().executeUpdate(SQL_TABLE);
        final PreparedStatement stmt = cnxCacheDisable.prepareStatement(SQL_INSERT);
        for (Object[] row : ROWS) {
            int i = 0;
            // For date/time/stamp columns
            Thread.sleep(10);
            final long ts = System.currentTimeMillis();

            stmt.setInt(i + 1, (Integer) row[i++]);
            stmt.setString(i + 1, (String) row[i++]);
            stmt.setTimestamp(i + 1, (Timestamp) (row[i++] = new Timestamp(ts)));
            stmt.setDate(i + 1, (Date) (row[i++] = new Date(ts)));
            stmt.setDate(i + 1, (Date) (row[i++] = new Date(ts)), Calendar.getInstance());
            stmt.setTime(i + 1, (Time) (row[i++] = new Time(ts)));
            stmt.setTime(i + 1, (Time) (row[i++] = new Time(ts)), Calendar.getInstance());
            stmt.setDouble(i + 1, (double) row[i++]);
            stmt.setFloat(i + 1, (float) row[i++]);
            stmt.setShort(i + 1, (short) row[i++]);
            stmt.setLong(i + 1, (long) row[i++]);
            final String clob = (String) row[i++];
            stmt.setClob(i, clob == null ? null : new StringReader(clob));
            Assert.assertEquals(1, stmt.executeUpdate());
        }
    }

    private void checkColString(final Object object, final int i, final ResultSet resultSet) throws SQLException {
        if (object == null) {
            Assert.assertNull(resultSet.getString(COLUMNS[i]));
            Assert.assertNull(resultSet.getString(i + 1));
        } else {
            Assert.assertEquals(object.toString(), resultSet.getString(COLUMNS[i]));
            Assert.assertEquals(object.toString(), resultSet.getString(i + 1));
        }
    }

    private void checkWasNull(final Object object, final int i, final ResultSet resultSet) throws SQLException {
        if (object == null)
            Assert.assertTrue(resultSet.wasNull());
        else
            Assert.assertFalse(resultSet.wasNull());
    }

    private ResultSet checkCache(ResultSet resultSet) {
        Assert.assertEquals("com.qwazr.jdbc.cache.CachedResultSet", resultSet.getClass().getName());
        return resultSet;
    }

    private ResultSet checkNoCache(ResultSet resultSet) {
        Assert.assertNotEquals("com.qwazr.jdbc.cache.CachedResultSet", resultSet.getClass().getName());
        return resultSet;
    }

    private void checkResultSet(ResultSet resultSet, Object[]... rows) throws SQLException, IOException {
        Assert.assertNotNull("The resultSet is null", resultSet);

        if (resultSet.getType() != ResultSet.TYPE_FORWARD_ONLY)
            Assert.assertTrue(resultSet.isBeforeFirst());

        int count = 0;
        while (resultSet.next()) {
            int i = 0;
            int j = 1;
            final Object[] row = rows[count];

            if (resultSet.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                if (count == 0)
                    Assert.assertTrue(resultSet.isFirst());
                else
                    Assert.assertFalse(resultSet.isFirst());
            }

            Assert.assertEquals(row[i], resultSet.getInt(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getInt(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getString(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getString(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getTimestamp(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getTimestamp(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i].toString(), resultSet.getDate(COLUMNS[i]).toString());
            Assert.assertEquals(row[i].toString(), resultSet.getDate(j++).toString());
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i].toString(), resultSet.getDate(COLUMNS[i], Calendar.getInstance()).toString());
            Assert.assertEquals(row[i].toString(), resultSet.getDate(j++, Calendar.getInstance()).toString());
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i].toString(), resultSet.getTime(COLUMNS[i]).toString());
            Assert.assertEquals(row[i].toString(), resultSet.getTime(j++).toString());
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i].toString(), resultSet.getTime(COLUMNS[i], Calendar.getInstance()).toString());
            Assert.assertEquals(row[i].toString(), resultSet.getTime(j++, Calendar.getInstance()).toString());
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getDouble(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getDouble(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getFloat(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getFloat(j++));
            // Disabled because precision issue
            // checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getShort(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getShort(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            Assert.assertEquals(row[i], resultSet.getLong(COLUMNS[i]));
            Assert.assertEquals(row[i], resultSet.getLong(j++));
            checkColString(row[i], i, resultSet);
            checkWasNull(row[i], i++, resultSet);

            //CLob
            Clob clob = resultSet.getClob(COLUMNS[i]);
            if (clob == null)
                Assert.assertNull(row[i]);
            else {
                Assert.assertEquals(row[i], clob.getSubString(1, (int) clob.length()));
                Assert.assertEquals(row[i], IOUtils.toString(clob.getCharacterStream(1, (int) clob.length())));
                Assert.assertEquals(row[i], IOUtils.toString(clob.getCharacterStream()));
                Assert.assertEquals(row[i], IOUtils.toString(clob.getAsciiStream(), Charset.defaultCharset()));
            }
            checkWasNull(row[i], i++, resultSet);

            count++;
        }
        Assert.assertEquals(rows.length, count);

        if (resultSet.getType() != ResultSet.TYPE_FORWARD_ONLY)
            Assert.assertTrue(resultSet.isAfterLast());

        Assert.assertFalse(resultSet.isClosed());
        resultSet.close();
        Assert.assertTrue(resultSet.isClosed());
    }

    final static String SQL_SIMPLE = "SELECT * FROM FIRSTTABLE";

    private ResultSet executeQuery(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        return stmt.executeQuery(SQL_SIMPLE);
    }

    @Test
    public void test110TestSimpleStatement() throws SQLException, IOException {
        // First without the cache
        checkResultSet(checkNoCache(executeQuery(cnxCacheDisable)), ROWS);
        // Second the cache is written
        checkResultSet(checkCache(executeQuery(cnxCacheEnable)), ROWS);
        // Third the cache is read
        checkResultSet(checkCache(executeQuery(cnxCacheEnable)), ROWS);
        // Last, without the backend
        checkResultSet(checkCache(executeQuery(cnxCacheNoBackend)), ROWS);
    }

    private ResultSet updateGetResultSet(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        stmt.execute(SQL_SIMPLE);
        return stmt.getResultSet();
    }

    @Test
    public void test110TestUpdateAndGetResultSet() throws SQLException, IOException {
        checkResultSet(checkNoCache(updateGetResultSet(cnxCacheDisable)), ROWS);
        checkResultSet(checkCache(updateGetResultSet(cnxCacheEnable)), ROWS);
        checkResultSet(checkCache(updateGetResultSet(cnxCacheNoBackend)), ROWS);
    }

    final static String SQL_PREP_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = ? OR ID = ?";

    private PreparedStatement getPreparedStatement(Connection cnx, Object[] row1, Object[] row2) throws SQLException {
        final PreparedStatement stmt = cnx.prepareStatement(SQL_PREP_ARG);
        stmt.setInt(1, (int) row1[0]);
        stmt.setInt(2, (int) row2[0]);
        return stmt;
    }

    final static String SQL_PREP_NO_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = " + ROW2[0];

    private CallableStatement getCallableStatement(Connection cnx) throws SQLException {
        final CallableStatement stmt = cnx.prepareCall(SQL_PREP_NO_ARG);
        return stmt;
    }

    @Test
    public void test110TestPreparedStatementWithoutCache() throws SQLException, IOException {
        checkResultSet(checkNoCache(getPreparedStatement(cnxCacheDisable, ROW1, ROW4).executeQuery()), ROW1, ROW4);
    }

    @Test
    public void test110TestCallableStatementWithoutCache() throws SQLException, IOException {
        checkResultSet(checkNoCache(getCallableStatement(cnxCacheDisable).executeQuery()), ROW2);
    }

    @Test
    public void test120TestPreparedStatementWithCache() throws SQLException, IOException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheEnable, ROW1, ROW4);
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
    }

    @Test
    public void test120TestCallableStatementWithCache() throws SQLException, IOException {
        final CallableStatement stmt = getCallableStatement(cnxCacheEnable);
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), ROW2);
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), ROW2);
    }

    @Test
    public void test130TestPreparedStatementNoBackend() throws SQLException, IOException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheNoBackend, ROW1, ROW4);
        checkResultSet(checkCache(stmt.executeQuery()), ROW1, ROW4);
    }

    @Test
    public void test130TestCallableStatementNoBackend() throws SQLException, IOException {
        final PreparedStatement stmt = getCallableStatement(cnxCacheNoBackend);
        checkResultSet(checkCache(stmt.executeQuery()), ROW2);
    }

    @Test
    public void test135TestPreparedStatementGetResultSet() throws SQLException, IOException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheNoBackend, ROW1, ROW4);
        stmt.execute();
        checkResultSet(checkCache(stmt.getResultSet()), ROW1, ROW4);
    }

    @Test
    public void test135TestCallableStatementGetResultSet() throws SQLException, IOException {
        final CallableStatement stmt = getCallableStatement(cnxCacheEnable);
        stmt.execute();
        checkResultSet(checkCache(stmt.getResultSet()), ROW2);
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
                            test110TestPreparedStatementWithoutCache();
                            test120TestPreparedStatementWithCache();
                            count.incrementAndGet();
                        } catch (SQLException | IOException e) {
                            throw new RuntimeException(e);
                        }
                });
                executor.submit(() -> {
                    while (System.currentTimeMillis() < exitTime)
                        try {
                            test110TestCallableStatementWithoutCache();
                            test120TestCallableStatementWithCache();
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
        final ResultSetMetaData metaData = getPreparedStatement(cnxCacheEnable, ROW1, ROW4).executeQuery()
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

    private interface RunnableEx {

        void run() throws SQLException;
    }

    private void checkNotSupported(RunnableEx runnable) throws SQLException {
        try {
            runnable.run();
            Assert.fail("SQLFeatureNotSupportedException not thrown");
        } catch (SQLFeatureNotSupportedException e) {
        }
    }

    @Test
    public void test800ResultSetNotSupportedMethod() throws SQLException {
        ResultSet resultSet = getPreparedStatement(cnxCacheEnable, ROW1, ROW4).executeQuery();
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
    public void test900TestCacheAPI() throws SQLException {
        ResultSetCache cache = com.qwazr.jdbc.cache.Driver.getCache(cnxCacheEnable);
        Assert.assertNotNull(cache);
        Assert.assertEquals(3, cache.size());

        Assert.assertEquals(0, cache.active());

        Statement stmt = getPreparedStatement(cnxCacheEnable, ROW1, ROW4);
        Assert.assertTrue(cache.exists(stmt));

        Assert.assertFalse(cache.active(stmt));

        cache.flush(stmt);
        Assert.assertEquals(2, cache.size());

        cache.flush();
        Assert.assertEquals(0, cache.size());
    }

}
