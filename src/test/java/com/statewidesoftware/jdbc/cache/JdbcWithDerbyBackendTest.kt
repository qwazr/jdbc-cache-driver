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

import com.statewidesoftware.jdbc.cache.DbTestUtil.RunnableEx
import com.statewidesoftware.jdbc.cache.DbTestUtil.checkNotSupported
import com.statewidesoftware.jdbc.cache.DbTestUtil.checkResultSet
import com.statewidesoftware.jdbc.cache.DbTestUtil.executeQuery
import com.statewidesoftware.jdbc.cache.DbTestUtil.getCallableStatement
import com.statewidesoftware.jdbc.cache.DbTestUtil.getPreparedStatement
import com.statewidesoftware.jdbc.cache.DbTestUtil.initTestDb
import com.statewidesoftware.jdbc.cache.DbTestUtil.updateGetResultSet
import com.statewidesoftware.jdbc.cache.Driver.Companion.getCache
import org.junit.*
import java.io.IOException
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

abstract class JdbcWithDerbyBackendTest : JdbcTest() {
    abstract val derbyDbName: String?
    abstract fun expectedResultSetClass(): Class<out ResultSet?>
    abstract val isCacheEnabled: Boolean

    val logger = java.util.logging.Logger.getLogger(this::class.java.name)

    /**
     * We make sure before starting a test that we have all the connections available
     * and that the database has been initialized
     *
     * @throws SQLException in case we can't execute things on the database
     */
    @Before
    @Throws(SQLException::class)
    fun createConnectionsAndSampleDb() {
        if (!dbCreated) {
            // Init the backend database so we can run our tests on it
            dbCreated = initTestDb(derbyDbName!!)
        }
    }

    @Test
    @Throws(SQLException::class, IOException::class)
    fun test110TestSimpleStatement() {
        // First the cache might be written
        checkResultSet(checkCache(executeQuery(connection)), *DbTestUtil.ROWS)
        // Second the cache might be read
        checkResultSet(checkCache(executeQuery(connection)), *DbTestUtil.ROWS)
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled) {
            checkResultSet(checkCache(executeQuery(noBackendConnection)), *DbTestUtil.ROWS)
        }
    }

    @Test
    @Throws(SQLException::class, IOException::class)
    fun test110TestUpdateAndGetResultSet() {
        checkResultSet(checkCache(updateGetResultSet(connection)), *DbTestUtil.ROWS)
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled) {
            checkResultSet(checkCache(updateGetResultSet(connection)), *DbTestUtil.ROWS)
        }
    }

    @Test
    @Throws(SQLException::class, IOException::class)
    fun test120TestPreparedStatement() {
        val stmt = getPreparedStatement(connection, DbTestUtil.ROW1, DbTestUtil.ROW4)
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW1, DbTestUtil.ROW4)
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW1, DbTestUtil.ROW4)
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled) {
            checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW1, DbTestUtil.ROW4)
        }
    }

    @Test
    @Throws(SQLException::class, IOException::class)
    fun test120TestCallableStatement() {
        val stmt = getCallableStatement(connection)
        // First the cache is written
        checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW2)
        // Second the cache is read
        checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW2)
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled) {
            checkResultSet(checkCache(stmt.executeQuery()), DbTestUtil.ROW2)
        }
    }

    @Test
    @Throws(SQLException::class, IOException::class)
    fun test135TestCallableStatementGetResultSet() {
        val stmt = getCallableStatement(connection)
        stmt.execute()
        checkResultSet(checkCache(stmt.resultSet), DbTestUtil.ROW2)
        // Try without any backend: the cache should be read if enabled
        if (isCacheEnabled) {
            checkResultSet(checkCache(stmt.resultSet), DbTestUtil.ROW2)
        }
    }

    @Test
    @Throws(InterruptedException::class)
    fun test500ThreadSafeTest() {
        val executor = Executors.newFixedThreadPool(8)
        val exitTime = System.currentTimeMillis() + 10 * 1000
        val count = AtomicInteger()
        try {
            for (i in 0..7) {
                executor.submit {
                    while (System.currentTimeMillis() < exitTime) try {
                        test120TestPreparedStatement()
                        count.incrementAndGet()
                    } catch (e: SQLException) {
                        throw RuntimeException(e)
                    } catch (e: IOException) {
                        throw RuntimeException(e)
                    }
                }
                executor.submit {
                    while (System.currentTimeMillis() < exitTime) try {
                        test120TestCallableStatement()
                        count.incrementAndGet()
                    } catch (e: SQLException) {
                        throw RuntimeException(e)
                    } catch (e: IOException) {
                        throw RuntimeException(e)
                    }
                }
            }
            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.HOURS)
            LOGGER.info("Iteration count: " + count.get())
        } finally {
            if (!executor.isShutdown) executor.shutdownNow()
        }
    }

    @Test
    @Throws(SQLException::class)
    fun test600TestResultSetMetaData() {
        getPreparedStatement(connection, DbTestUtil.ROW1, DbTestUtil.ROW4).executeQuery().use { resultSet ->
            Assert.assertNotNull(resultSet)
            val metaData = resultSet.metaData
            Assert.assertNotNull(metaData)
            val colCount = metaData.columnCount
            Assert.assertTrue(colCount > 0)
            for (i in 1..colCount) {
                Assert.assertNotNull(metaData.getColumnName(i))
                Assert.assertNotNull(metaData.getColumnLabel(i))
                Assert.assertNotNull(metaData.getColumnTypeName(i))
                Assert.assertNotNull(metaData.getCatalogName(i))
                Assert.assertNotNull(metaData.getColumnClassName(i))
                Assert.assertTrue(metaData.getColumnDisplaySize(i) > 0)
                Assert.assertFalse(metaData.isReadOnly(i))
                Assert.assertNotNull(metaData.getSchemaName(i))
                Assert.assertNotNull(metaData.getTableName(i))
                Assert.assertTrue(metaData.getPrecision(i) > 0)
                Assert.assertTrue(metaData.isSearchable(i))
                if (i == 1) {
                    Assert.assertTrue(metaData.isSigned(i))
                    Assert.assertEquals(0, metaData.getScale(i).toLong())
                    Assert.assertEquals(ResultSetMetaData.columnNoNulls.toLong(), metaData.isNullable(i).toLong())
                    Assert.assertFalse(metaData.isCaseSensitive(i))
                    Assert.assertFalse(metaData.isAutoIncrement(i))
                    Assert.assertFalse(metaData.isCurrency(i))
                    Assert.assertFalse(metaData.isWritable(i))
                } else {
                    Assert.assertEquals(ResultSetMetaData.columnNullable.toLong(), metaData.isNullable(i).toLong())
                }
            }
        }
    }

    @Test
    @Throws(SQLException::class)
    fun test800ResultSetNotSupportedMethod() {
        getPreparedStatement(connection, DbTestUtil.ROW1, DbTestUtil.ROW4).executeQuery().use { resultSet ->
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateArray(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateArray("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateAsciiStream(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateAsciiStream("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBigDecimal(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBigDecimal("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateByte(1, 0.toByte())
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateByte("id", 0.toByte())
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBytes(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBytes("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBoolean(1, false)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateBoolean("id", false)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateDate(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateDate("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateDouble(1, 0.0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateDouble("id", 0.0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateFloat(1, 0f)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateFloat("id", 0f)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateInt(1, 0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateInt("id", 0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateLong(1, 0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateLong("id", 0)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateRowId(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateRowId("id", null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateNull(1)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateNull("id")
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateObject(1, null)
                }
            })
            checkNotSupported(object : RunnableEx {
                override fun run() {
                    resultSet.updateObject("id", null)
                }
            })
        }
    }

    @Test
    @Throws(SQLException::class)
    fun test900TestCacheAPI() {
        val cache = getCache(connection)

        // We can only test the cache API in the context of a cacheable connection
        Assume.assumeNotNull(cache)

        // Empty the cache if anything has been ran before
        cache!!.flush()
        Assert.assertEquals(0, cache.size().toLong())
        Assert.assertEquals(0, cache.active().toLong())
        executeQuery(connection).close()
        getPreparedStatement(connection, DbTestUtil.ROW1, DbTestUtil.ROW4).executeQuery().close()
        getCallableStatement(connection).executeQuery().close()
        Assert.assertEquals(3, cache.size().toLong())
        Assert.assertEquals(0, cache.active().toLong())
        val stmt: Statement = getPreparedStatement(connection, DbTestUtil.ROW1, DbTestUtil.ROW4)
        Assert.assertTrue(cache.exists(stmt))
        Assert.assertFalse(cache.active(stmt))
        cache.flush(stmt)
        Assert.assertEquals(2, cache.size().toLong())
        cache.flush()
        Assert.assertEquals(0, cache.size().toLong())
    }

    private fun checkCache(resultSet: ResultSet): ResultSet {
        Assert.assertEquals(expectedResultSetClass().name, resultSet.javaClass.name)
        return resultSet
    }

    companion object {
        private var dbCreated = false
        @JvmStatic
        @BeforeClass
        fun init() {
            dbCreated = false
        }
    }
}