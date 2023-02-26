package com.statewidesoftware.jdbc.cache/*
 * Copyright 2016-2017 Emmanuel Keller / QWAZR
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

import java.io.IOException
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

internal class ResultSetInMemoryCacheImpl : ResultSetCacheImpl() {
    private val activeKeys: ConcurrentHashMap<String?, ReentrantLock>
    private val cache: ConcurrentHashMap<String?, ByteArray>

    init {
        activeKeys = ConcurrentHashMap()
        cache = ConcurrentHashMap()
    }

    /**
     * Return the cached ResultSet for the given key.
     * If the entry does not exist the ResultSet is extracted by calling the given resultSetProvider.
     * If the entry does not exist and no resultSetProvider is given (null) an SQLException is thrown.
     *
     * @param statement         the cached statement
     * @param key               the generated key for this statement
     * @param resultSetProvider the optional result provider
     * @return the cached ResultSet
     * @throws SQLException if the statement cannot be executed
     */
    @Throws(SQLException::class)
    override operator fun <T> get(
        statement: CachedStatement<*>?,
        key: String?,
        resultSetProvider: ResultSetCache.Provider?
    ): ResultSet? {
        if (!cache.containsKey(key)) {
            if (resultSetProvider == null) throw SQLException("No cache available")
            try {
                buildCache(key, resultSetProvider)
            } catch (e: IOException) {
                throw SQLException("Can not read cache", e)
            }
        }
        return CachedInMemoryResultSet(statement, cache[key])
    }

    @Throws(SQLException::class, IOException::class)
    private fun buildCache(key: String?, resultSetProvider: ResultSetCache.Provider) {
        val keyLock: Lock = activeKeys.computeIfAbsent(key) { s: String? -> ReentrantLock(true) }
        try {
            keyLock.lock()
            try {
                val providedResultSet = resultSetProvider.provide()
                if (providedResultSet == null) {
                    cache[key!!] = ByteArray(0)
                    return
                }
                val outputStream = ResultSetWriter.write(providedResultSet)
                cache[key!!] = outputStream.toByteArray()
            } finally {
                keyLock.unlock()
            }
        } finally {
            activeKeys.remove(key)
        }
    }

    /**
     * Check if an entry is available for this key.
     *
     * @param key the computed key
     * @return always true if the cache entry exists
     */
    override fun checkIfExists(key: String?): Boolean {
        return cache.containsKey(key)
    }

    @Throws(SQLException::class)
    override fun flush() {
        cache.clear()
    }

    @Throws(SQLException::class)
    override fun flush(stmt: Statement?) {
        cache.remove(checkKey(stmt))
    }

    @Throws(SQLException::class)
    override fun size(): Int {
        return cache.size
    }

    @Throws(SQLException::class)
    override fun exists(stmt: Statement?): Boolean {
        return cache.containsKey(checkKey(stmt))
    }
}