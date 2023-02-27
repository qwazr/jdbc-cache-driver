package io.github.jhstatewide.jdbc.cache/*
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

import java.io.ByteArrayOutputStream
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

internal abstract class ResultSetCacheImpl : ResultSetCache {
    private val activeKeys: ConcurrentHashMap<String?, ReentrantLock> = ConcurrentHashMap()
    private val cache: ConcurrentHashMap<String?, ByteArrayOutputStream> = ConcurrentHashMap()

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

    private fun checkCacheMap(): ConcurrentHashMap<String?, ReentrantLock> {
        return Objects.requireNonNull(activeKeys, "No cache")
    }

    @Throws(SQLException::class)
    private fun checkCachedStatement(stmt: Statement?): CachedStatement<*> {
        Objects.requireNonNull(stmt, "The statement is null")
        if (stmt is CachedStatement<*>) return stmt
        throw SQLException("The statement is not cached")
    }

    @Throws(SQLException::class)
    fun checkKey(stmt: Statement?): String? {
        return Objects.requireNonNull(checkCachedStatement(stmt).orGenerateKey, "No key found")
    }

    override fun active(): Int {
        return checkCacheMap().size
    }

    @Throws(SQLException::class)
    override fun active(stmt: Statement?): Boolean {
        return checkCacheMap().containsKey(checkKey(stmt))
    }
}