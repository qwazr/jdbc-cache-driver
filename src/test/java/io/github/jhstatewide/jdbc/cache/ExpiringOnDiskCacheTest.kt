package io.github.jhstatewide.jdbc.cache

import io.github.jhstatewide.jdbc.cache.Driver.Companion.CACHE_DRIVER_MAX_AGE
import org.junit.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.Properties

class ExpiringOnDiskCacheTest: OnDiskCacheEnabledJdbcWithDerbyBackendTest() {

    override val isCacheEnabled: Boolean
        get() = true

    override val derbyDbName: String
        get() = "onDiskCacheEnabled"

    override fun expectedResultSetClass(): Class<out ResultSet?> {
        return CachedOnDiskResultSet::class.java
    }

    @Test
    fun testCacheExpiry() {
        val expirationListener = object: ExpirationListener {
            var numberExpired = 0
            override fun onExpiration(key: String) {
                numberExpired++
            }
        }

        ExpirationEventBus.addListener(expirationListener)

        val connection = getConnection()
        val statement = connection.createStatement()
        statement.execute("CREATE TABLE test (id INT, name VARCHAR(255))")
        statement.execute("INSERT INTO test VALUES (1, 'test')")
        val resultSet = statement.executeQuery("SELECT * FROM test")
        resultSet.next()
        resultSet.getInt(1)
        resultSet.getString(2)
        resultSet.close()
        statement.close()
        connection.close()
        Thread.sleep(2000)
        val connection2 = getConnection()
        val statement2 = connection2.createStatement()
        val resultSet2 = statement2.executeQuery("SELECT * FROM test")
        resultSet2.next()
        resultSet2.getInt(1)
        resultSet2.getString(2)
        resultSet2.close()
        statement2.close()
        connection2.close()
        assert(expirationListener.numberExpired == 1)
    }

    override fun getConnection(): Connection {
        val info = Properties()
        // set max age to 1 second
        info.setProperty(CACHE_DRIVER_MAX_AGE, "1")
        info.setProperty("cache.driver.url", "jdbc:derby:memory:$derbyDbName;create=true")
        return DriverManager.getConnection(orSetJdbcCacheUrl, info)
    }
}
