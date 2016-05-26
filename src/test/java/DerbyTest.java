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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DerbyTest {

    private static Connection cnxCacheDisable;
    private static Connection cnxCacheEnable;
    private static Connection cnxCacheNoBackend;

    private static String jdbcCacheUrl;

    @BeforeClass
    public static void init() throws ClassNotFoundException, IOException {
        Class.forName("com.qwazr.jdbc.cache.Driver");
        final Path tempDir = Files.createTempDirectory("jdbc-cache-test");
        jdbcCacheUrl = "jdbc:cache:file:" + tempDir.toUri().getPath();
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

    final static Object[] ROW1 = { 10, "TEN", null };
    final static Object[] ROW2 = { 20, "TWENTY", null };
    final static Object[] ROW3 = { 30, "THIRTY", null };
    final static Object[] ROW4 = { 40, null, null };
    final static Object[] ROW5 = { 50, null, null };

    final static Object[][] ROWS = { ROW1, ROW2, ROW3, ROW4, ROW5 };

    final static String SQL_TABLE = "CREATE TABLE FIRSTTABLE (ID INT PRIMARY KEY, NAME VARCHAR(12), CREATED TIMESTAMP)";
    final static String SQL_INSERT = "INSERT INTO FIRSTTABLE VALUES (?,?,?)";

    @Test
    public void test100createTableAndDataSet() throws SQLException {
        cnxCacheDisable.createStatement().executeUpdate(SQL_TABLE);
        final PreparedStatement stmt = cnxCacheDisable.prepareStatement(SQL_INSERT);
        for (Object[] row : ROWS) {
            stmt.setInt(1, (Integer) row[0]);
            stmt.setString(2, (String) row[1]);
            row[2] = new Timestamp(System.currentTimeMillis());
            stmt.setTimestamp(3, (Timestamp) row[2]);
            Assert.assertEquals(1, stmt.executeUpdate());
        }
    }

    private void checkResultSet(ResultSet resultSet, Object[]... rows) throws SQLException {
        Assert.assertNotNull("The resultSet is null", resultSet);
        int count = 0;
        while (resultSet.next()) {
            Assert.assertEquals(rows[count][0], resultSet.getInt(1));
            Assert.assertEquals(rows[count][1], resultSet.getString(2));
            Assert.assertEquals(rows[count][2], resultSet.getTimestamp(3));
            count++;
        }
        Assert.assertEquals(rows.length, count);
    }

    final static String SQL_SIMPLE = "SELECT ID,NAME,CREATED  FROM FIRSTTABLE";

    private ResultSet executeQuery(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        return stmt.executeQuery(SQL_SIMPLE);
    }

    @Test
    public void test110TestSimpleStatement() throws SQLException {
        // First without the cache
        checkResultSet(executeQuery(cnxCacheDisable), ROWS);
        // Second the cache is written
        checkResultSet(executeQuery(cnxCacheEnable), ROWS);
        // Third the cache is read
        checkResultSet(executeQuery(cnxCacheEnable), ROWS);
        // Last, without the backend
        checkResultSet(executeQuery(cnxCacheNoBackend), ROWS);
    }

    private ResultSet updateGetResultSet(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        stmt.execute(SQL_SIMPLE);
        return stmt.getResultSet();
    }

    @Test
    public void test110TestUpdateAndGetResultSet() throws SQLException {
        checkResultSet(updateGetResultSet(cnxCacheDisable), ROWS);
        checkResultSet(updateGetResultSet(cnxCacheEnable), ROWS);
        checkResultSet(updateGetResultSet(cnxCacheNoBackend), ROWS);
    }

    final static String SQL_PREP = "SELECT ID,NAME,CREATED  FROM FIRSTTABLE WHERE ID = ? OR ID = ?";

    private PreparedStatement getPreparedStatement(Connection cnx) throws SQLException {
        final PreparedStatement stmt = cnx.prepareStatement(SQL_PREP);
        stmt.setInt(1, 10);
        stmt.setInt(2, 40);
        return stmt;
    }

    @Test
    public void test110TestPreparedStatementWithoutCache() throws SQLException {
        checkResultSet(getPreparedStatement(cnxCacheDisable).executeQuery(), ROW1, ROW4);
    }

    @Test
    public void test120TestPreparedStatementWithCache() throws SQLException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheEnable);
        // First the cache is written
        checkResultSet(stmt.executeQuery(), ROW1, ROW4);
        // Second the cache is read
        checkResultSet(stmt.executeQuery(), ROW1, ROW4);
    }

    @Test
    public void test130TestPreparedStatementNoBackend() throws SQLException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheNoBackend);
        checkResultSet(stmt.executeQuery(), ROW1, ROW4);
    }

    @Test
    public void test135TestPreparedStatementGetResultSet() throws SQLException {
        final PreparedStatement stmt = getPreparedStatement(cnxCacheNoBackend);
        stmt.execute();
        checkResultSet(stmt.getResultSet(), ROW1, ROW4);
    }

}
