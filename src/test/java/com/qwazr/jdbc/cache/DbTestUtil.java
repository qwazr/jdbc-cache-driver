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

import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

class DbTestUtil {

    final static Object[] ROW1 = { 10, "TEN", null, null, null, null, null, 1.11D, 1.1F, (short) 1, 1000000L, "clob1" };
    final static Object[] ROW2 = { 20, "TWENTY", null, null, null, null, null, 2.22D, 2.2F, (short) 2, 2000000L,
            "clob2" };
    final static Object[] ROW3 = { 30, "THIRTY", null, null, null, null, null, 3.33D, 3.3F, (short) 3, 3000000L,
            "clob3" };
    final static Object[] ROW4 = { 40, null, null, null, null, null, null, 4.44D, 4.4F, (short) 4, 4000000L, "clob4" };
    final static Object[] ROW5 = { 50, null, null, null, null, null, null, 5.55D, 5.5F, (short) 5, 5000000L, null };

    final static Object[][] ROWS = { ROW1, ROW2, ROW3, ROW4, ROW5 };

    final static String SQL_TABLE = "CREATE TABLE FIRSTTABLE (ID INT PRIMARY KEY, NAME VARCHAR(12), TS TIMESTAMP, "
            + "DT1 DATE, DT2 DATE, TI1 TIME, TI2 TIME, DBL DOUBLE, FL FLOAT, TI SMALLINT, BI BIGINT, CL CLOB)";
    final static String SQL_INSERT = "INSERT INTO FIRSTTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

    final static String[] COLUMNS = { "ID", "NAME", "TS", "DT1", "DT2", "TI1", "TI2", "DBL", "FL", "TI", "BI", "CL" };

    final static String SQL_SIMPLE = "SELECT * FROM FIRSTTABLE";

    final static String SQL_PREP_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = ? OR ID = ?";

    final static String SQL_PREP_NO_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = " + ROW2[0];

    static boolean initTestDb(String dbName) throws SQLException {
        // Init the backend database so we can run our tests on it
        Connection connection = DriverManager.getConnection("jdbc:derby:memory:" + dbName + ";create=true");
        connection.createStatement().executeUpdate(SQL_TABLE);
        final PreparedStatement stmt = connection.prepareStatement(SQL_INSERT);
        for (Object[] row : ROWS) {
            int i = 0;
            // For date/time/stamp columns
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
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
        return true;
    }

    static void checkColString(final Object object, final int i, final ResultSet resultSet) throws SQLException {
        if (object == null) {
            Assert.assertNull(resultSet.getString(COLUMNS[i]));
            Assert.assertNull(resultSet.getString(i + 1));
        } else {
            Assert.assertEquals(object.toString(), resultSet.getString(COLUMNS[i]));
            Assert.assertEquals(object.toString(), resultSet.getString(i + 1));
        }
    }

    static void checkWasNull(final Object object, final int i, final ResultSet resultSet) throws SQLException {
        if (object == null)
            Assert.assertTrue(resultSet.wasNull());
        else
            Assert.assertFalse(resultSet.wasNull());
    }

    static void checkResultSet(ResultSet resultSet, Object[]... rows) throws SQLException, IOException {
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

    static PreparedStatement getPreparedStatement(Connection cnx, Object[] row1, Object[] row2) throws SQLException {
        final PreparedStatement stmt = cnx.prepareStatement(SQL_PREP_ARG);
        stmt.setInt(1, (int) row1[0]);
        stmt.setInt(2, (int) row2[0]);
        return stmt;
    }

    static ResultSet executeQuery(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        return stmt.executeQuery(SQL_SIMPLE);
    }

    static CallableStatement getCallableStatement(Connection cnx) throws SQLException {
        return cnx.prepareCall(SQL_PREP_NO_ARG);
    }

    static ResultSet updateGetResultSet(Connection cnx) throws SQLException {
        final Statement stmt = cnx.createStatement();
        stmt.execute(SQL_SIMPLE);
        return stmt.getResultSet();
    }

    interface RunnableEx {
        void run() throws SQLException;
    }

    static void checkNotSupported(RunnableEx runnable) throws SQLException {
        try {
            runnable.run();
            Assert.fail("SQLFeatureNotSupportedException or SQLException not thrown");
        } catch (SQLFeatureNotSupportedException ignored) {
        } catch (SQLException ignored) {
            // Some Drivers like Derby don't throw SQLFeatureNotSupportedException but SQLException
            Assert.assertThat(ignored.getMessage(), CoreMatchers.containsString("not allowed because"));
        }
    }
}
