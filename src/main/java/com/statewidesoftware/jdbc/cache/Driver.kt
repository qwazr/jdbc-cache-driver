/*
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
package com.statewidesoftware.jdbc.cache

import java.nio.file.FileSystems
import java.sql.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger

class Driver : java.sql.Driver {
    private val resultSetCacheMap = ConcurrentHashMap<String, ResultSetCache>()
    @Throws(SQLException::class, IllegalArgumentException::class)
    override fun connect(url: String, info: Properties): Connection? {
        if (!acceptsURL(url)) return null

        // Determine the optional backend connection
        val cacheDriverUrl = info.getProperty(CACHE_DRIVER_URL)
        val cacheDriverClass = info.getProperty(CACHE_DRIVER_CLASS)
        try {
            if (cacheDriverClass != null && cacheDriverClass.isNotEmpty()) Class.forName(cacheDriverClass)
        } catch (e: ClassNotFoundException) {
            throw SQLException("Cannot initialize the driver: $cacheDriverClass", e)
        }
        val cacheDriverActive = info.getProperty(CACHE_DRIVER_ACTIVE)
        val active = cacheDriverActive == null || java.lang.Boolean.parseBoolean(cacheDriverActive)
        val backendConnection =
            if (cacheDriverUrl == null || cacheDriverUrl.isEmpty()) null else DriverManager.getConnection(
                cacheDriverUrl,
                info
            )
        if (!active) {
            return CachedConnection(backendConnection, null)
        }
        val resultSetCache: ResultSetCache = if (url.startsWith(URL_FILE_PREFIX)) {
            if (url.length <= URL_FILE_PREFIX.length) {
                throw SQLException("The path is empty: $url")
            }
            // Check the cache directory
            val cacheName = url.substring(URL_FILE_PREFIX.length)
            val cacheDirectory = FileSystems.getDefault().getPath(cacheName)
            resultSetCacheMap.computeIfAbsent(cacheName) { ResultSetOnDiskCacheImpl(cacheDirectory) }
        } else if (url.startsWith(URL_MEM_PREFIX)) {
            if (url.length <= URL_MEM_PREFIX.length) {
                throw SQLException("The name is empty: $url")
            }
            // Check the cache directory
            val cacheName = url.substring(URL_MEM_PREFIX.length)
            resultSetCacheMap.computeIfAbsent(cacheName) { ResultSetInMemoryCacheImpl() }
        } else {
            throw IllegalArgumentException("Can not find cache implementation for $url")
        }
        return CachedConnection(backendConnection, resultSetCache)
    }

    @Throws(SQLException::class)
    override fun acceptsURL(url: String?): Boolean {
        return (url != null) && (url.startsWith(URL_FILE_PREFIX) || url.startsWith(URL_MEM_PREFIX))
    }

    @Throws(SQLException::class)
    override fun getPropertyInfo(url: String, info: Properties): Array<DriverPropertyInfo> {
        return arrayOf(
            DriverPropertyInfo(CACHE_DRIVER_URL, null),
            DriverPropertyInfo(CACHE_DRIVER_CLASS, null), DriverPropertyInfo(CACHE_DRIVER_ACTIVE, null)
        )
    }

    override fun getMajorVersion(): Int {
        return 1
    }

    override fun getMinorVersion(): Int {
        return 3
    }

    override fun jdbcCompliant(): Boolean {
        return false
    }

    @Throws(SQLFeatureNotSupportedException::class)
    override fun getParentLogger(): Logger {
        return LOGGER
    }

    companion object {
        val LOGGER = Logger.getLogger(Driver::class.java.getPackage().name)
        const val URL_FILE_PREFIX = "jdbc:cache:file:"
        const val URL_MEM_PREFIX = "jdbc:cache:mem:"
        const val CACHE_DRIVER_URL = "cache.driver.url"
        const val CACHE_DRIVER_CLASS = "cache.driver.class"
        const val CACHE_DRIVER_ACTIVE = "cache.driver.active"

        init {
            try {
                DriverManager.registerDriver(Driver())
            } catch (e: SQLException) {
                LOGGER.log(Level.SEVERE, e.message, e)
            }
        }

        @JvmStatic
        @Throws(SQLException::class)
        fun getCache(connection: Connection?): ResultSetCache? {
            if (connection !is CachedConnection) throw SQLException("The connection is not a cached connection")
            return connection.resultSetCache
        }
    }
}