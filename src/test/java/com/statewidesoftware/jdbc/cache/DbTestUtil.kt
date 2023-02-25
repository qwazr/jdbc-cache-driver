/**
 * Copyright 2016 Emmanuel Keller / QWAZR
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.statewidesoftware.jdbc.cache

import org.apache.commons.io.IOUtils
import org.hamcrest.CoreMatchers
import org.junit.Assert
import java.io.IOException
import java.io.StringReader
import java.nio.charset.Charset
import java.sql.*
import java.sql.Date
import java.util.*

internal object DbTestUtil {
    @JvmField
    val ROW1 = arrayOf<Any?>(10, "TEN", null, null, null, null, null, 1.11, 1.1f, 1.toShort(), 1000000L, "clob1")
    @JvmField
    val ROW2 = arrayOf<Any?>(
        20, "TWENTY", null, null, null, null, null, 2.22, 2.2f, 2.toShort(), 2000000L,
        "clob2"
    )
    val ROW3 = arrayOf<Any?>(
        30, "THIRTY", null, null, null, null, null, 3.33, 3.3f, 3.toShort(), 3000000L,
        "clob3"
    )
    @JvmField
    val ROW4 = arrayOf<Any?>(40, null, null, null, null, null, null, 4.44, 4.4f, 4.toShort(), 4000000L, "clob4")
    val ROW5 = arrayOf<Any?>(50, null, null, null, null, null, null, 5.55, 5.5f, 5.toShort(), 5000000L, null)
    @JvmField
    val ROWS = arrayOf(ROW1, ROW2, ROW3, ROW4, ROW5)
    const val SQL_TABLE = ("CREATE TABLE FIRSTTABLE (ID INT PRIMARY KEY, NAME VARCHAR(12), TS TIMESTAMP, "
            + "DT1 DATE, DT2 DATE, TI1 TIME, TI2 TIME, DBL DOUBLE, FL FLOAT, TI SMALLINT, BI BIGINT, CL CLOB)")
    const val SQL_INSERT = "INSERT INTO FIRSTTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
    val COLUMNS = arrayOf("ID", "NAME", "TS", "DT1", "DT2", "TI1", "TI2", "DBL", "FL", "TI", "BI", "CL")
    const val SQL_SIMPLE = "SELECT * FROM FIRSTTABLE"
    const val SQL_PREP_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = ? OR ID = ?"
    val SQL_PREP_NO_ARG = "SELECT * FROM FIRSTTABLE WHERE ID = " + ROW2[0]
    @JvmStatic
    @Throws(SQLException::class)
    fun initTestDb(dbName: String): Boolean {
        // Init the backend database so we can run our tests on it
        val connection = DriverManager.getConnection("jdbc:derby:memory:$dbName;create=true")
        connection.createStatement().executeUpdate(SQL_TABLE)
        val stmt = connection.prepareStatement(SQL_INSERT)
        for (row in ROWS) {
            var i = 0
            // For date/time/stamp columns
            try {
                Thread.sleep(10)
            } catch (ignored: InterruptedException) {
            }
            val ts = System.currentTimeMillis()
            stmt.setInt(i + 1, (row[i++] as Int))
            stmt.setString(i + 1, row[i++] as String?)
            stmt.setTimestamp(i + 1, Timestamp(ts).also { row[i++] = it })
            stmt.setDate(i + 1, Date(ts).also { row[i++] = it })
            stmt.setDate(i + 1, Date(ts).also { row[i++] = it }, Calendar.getInstance())
            stmt.setTime(i + 1, Time(ts).also { row[i++] = it })
            stmt.setTime(i + 1, Time(ts).also { row[i++] = it }, Calendar.getInstance())
            stmt.setDouble(i + 1, row[i++] as Double)
            stmt.setFloat(i + 1, row[i++] as Float)
            stmt.setShort(i + 1, row[i++] as Short)
            stmt.setLong(i + 1, row[i++] as Long)
            val clob = row[i++] as String?
            stmt.setClob(i, if (clob == null) null else StringReader(clob))
            Assert.assertEquals(1, stmt.executeUpdate().toLong())
        }
        return true
    }

    @Throws(SQLException::class)
    fun checkColString(`object`: Any?, i: Int, resultSet: ResultSet) {
        if (`object` == null) {
            Assert.assertNull(resultSet.getString(COLUMNS[i]))
            Assert.assertNull(resultSet.getString(i + 1))
        } else {
            Assert.assertEquals(`object`.toString(), resultSet.getString(COLUMNS[i]))
            Assert.assertEquals(`object`.toString(), resultSet.getString(i + 1))
        }
    }

    @Throws(SQLException::class)
    fun checkWasNull(`object`: Any?, resultSet: ResultSet) {
        if (`object` == null) Assert.assertTrue(resultSet.wasNull()) else Assert.assertFalse(resultSet.wasNull())
    }

    @JvmStatic
    @Throws(SQLException::class, IOException::class)
    fun checkResultSet(resultSet: ResultSet, vararg rows: Array<Any>) {
        Assert.assertNotNull("The resultSet is null", resultSet)
        if (resultSet.type != ResultSet.TYPE_FORWARD_ONLY) Assert.assertTrue(resultSet.isBeforeFirst)
        var count = 0
        while (resultSet.next()) {
            var i = 0
            var j = 1
            val row = rows[count]
            if (resultSet.type != ResultSet.TYPE_FORWARD_ONLY) {
                if (count == 0) Assert.assertTrue(resultSet.isFirst) else Assert.assertFalse(resultSet.isFirst)
            }
            Assert.assertEquals(row[i], resultSet.getInt(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getInt(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getString(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getString(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getTimestamp(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getTimestamp(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i].toString(), resultSet.getDate(COLUMNS[i]).toString())
            Assert.assertEquals(row[i].toString(), resultSet.getDate(j++).toString())
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i].toString(), resultSet.getDate(COLUMNS[i], Calendar.getInstance()).toString())
            Assert.assertEquals(row[i].toString(), resultSet.getDate(j++, Calendar.getInstance()).toString())
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i].toString(), resultSet.getTime(COLUMNS[i]).toString())
            Assert.assertEquals(row[i].toString(), resultSet.getTime(j++).toString())
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i].toString(), resultSet.getTime(COLUMNS[i], Calendar.getInstance()).toString())
            Assert.assertEquals(row[i].toString(), resultSet.getTime(j++, Calendar.getInstance()).toString())
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getDouble(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getDouble(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getFloat(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getFloat(j++))
            // Disabled because precision issue
            // checkColString(row[i], i, resultSet);
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getShort(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getShort(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)
            Assert.assertEquals(row[i], resultSet.getLong(COLUMNS[i]))
            Assert.assertEquals(row[i], resultSet.getLong(j++))
            checkColString(row[i], i, resultSet)
            checkWasNull(row[i++], resultSet)

            //CLob
            val clob = resultSet.getClob(COLUMNS[i])
            if (clob == null) Assert.assertNull(row[i]) else {
                Assert.assertEquals(row[i], clob.getSubString(1, clob.length().toInt()))
                Assert.assertEquals(
                    row[i], IOUtils.toString(clob.getCharacterStream(1, clob.length().toInt().toLong()))
                )
                Assert.assertEquals(row[i], IOUtils.toString(clob.characterStream))
                Assert.assertEquals(row[i], IOUtils.toString(clob.asciiStream, Charset.defaultCharset()))
            }
            checkWasNull(row[i++], resultSet)
            count++
        }
        Assert.assertEquals(rows.size.toLong(), count.toLong())
        if (resultSet.type != ResultSet.TYPE_FORWARD_ONLY) Assert.assertTrue(resultSet.isAfterLast)
        Assert.assertFalse(resultSet.isClosed)
        resultSet.close()
        Assert.assertTrue(resultSet.isClosed)
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun getPreparedStatement(cnx: Connection, row1: Array<Any>, row2: Array<Any>): PreparedStatement {
        val stmt = cnx.prepareStatement(SQL_PREP_ARG)
        stmt.setInt(1, row1[0] as Int)
        stmt.setInt(2, row2[0] as Int)
        return stmt
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun executeQuery(cnx: Connection): ResultSet {
        val stmt = cnx.createStatement()
        return stmt.executeQuery(SQL_SIMPLE)
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun getCallableStatement(cnx: Connection): CallableStatement {
        return cnx.prepareCall(SQL_PREP_NO_ARG)
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun updateGetResultSet(cnx: Connection): ResultSet {
        val stmt = cnx.createStatement()
        stmt.execute(SQL_SIMPLE)
        return stmt.resultSet
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun checkNotSupported(runnable: RunnableEx) {
        try {
            runnable.run()
            Assert.fail("SQLFeatureNotSupportedException or SQLException not thrown")
        } catch (ignored: SQLFeatureNotSupportedException) {
        } catch (ignored: SQLException) {
            // Some Drivers like Derby don't throw SQLFeatureNotSupportedException but SQLException
            Assert.assertThat(ignored.message, CoreMatchers.containsString("not allowed because"))
        }
    }

    internal interface RunnableEx {
        @Throws(SQLException::class)
        fun run()
    }
}