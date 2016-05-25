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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING) public class DerbyTest {

    private static Connection cnx;

    @Test
    public void test001initDriver() throws SQLException, IOException, ClassNotFoundException {
        Class.forName("com.qwazr.jdbc.cache.Driver");
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        final Path tempDir = Files.createTempDirectory("jdbc-cache-test");
        final String jdbcCacheUrl = "jdbc:cache:file:" + tempDir.toUri().getPath();
        final Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
        cnx = DriverManager.getConnection(jdbcCacheUrl, info);
        Assert.assertNotNull(cnx);
    }

    private int update(String sql) throws SQLException {
        return cnx.createStatement().executeUpdate(sql);
    }

    private void checkResultSet(ResultSet resultSet, int expectedCount) throws SQLException {
        Assert.assertNotNull("The resultSet is null", resultSet);
        int count = 0;
        while (resultSet.next())
            count++;
        Assert.assertEquals(expectedCount, count);
    }

    @Test
    public void test100createTableAndDataSet() throws SQLException {
        update("CREATE TABLE FIRSTTABLE (ID INT PRIMARY KEY, NAME VARCHAR(12))");
        Assert.assertEquals(3, update("INSERT INTO FIRSTTABLE VALUES (10,'TEN'),(20,'TWENTY'),(30,'THIRTY')"));
    }

    @Test
    public void test110TestSimpleStatement() throws SQLException {
        final String sql = "SELECT * FROM FIRSTTABLE";
        // First the cache is written
        checkResultSet(cnx.createStatement().executeQuery(sql), 3);
        // Second the cache is read
        checkResultSet(cnx.createStatement().executeQuery(sql), 3);
    }

    @Test
    public void test110TestPreparedStatement() throws SQLException {
        final String sql = "SELECT * FROM FIRSTTABLE WHERE ID = ?";
        final PreparedStatement stmt = cnx.prepareStatement(sql);
        stmt.setInt(0, 10);
        // First the cache is written
        checkResultSet(stmt.executeQuery(), 1);
        // Second the cache is read
        checkResultSet(stmt.executeQuery(), 1);
    }

}
