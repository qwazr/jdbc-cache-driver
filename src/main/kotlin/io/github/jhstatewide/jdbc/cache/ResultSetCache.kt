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

import java.io.IOException
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

interface ResultSetCache {
    /**
     * Flush all entries in the cache
     *
     * @throws SQLException if any SQL error occurs
     */
    @Throws(SQLException::class)
    fun flush()

    /**
     * Remove any cache entry for the given statement.
     *
     * @param stmt the statement to flush
     * @throws SQLException if any SQL error occurs
     */
    @Throws(SQLException::class)
    fun flush(stmt: Statement?)

    /**
     * @return the number of entries in the cache
     * @throws SQLException if any SQL error occurs
     */
    @Throws(SQLException::class)
    fun size(): Int

    /**
     * Check if the cache contains an entry for this statement.
     *
     * @param stmt the statement to check
     * @return true if a cache entry exists
     * @throws SQLException if any SQL error occurs
     */
    @Throws(SQLException::class)
    fun exists(stmt: Statement?): Boolean

    /**
     * @return the number of cache entry build currently in progress
     */
    fun active(): Int

    /**
     * @param stmt the statement to flush
     * @return true if a cache entry is currently build for the given statement
     * @throws SQLException if any SQL error occurs
     */
    @Throws(SQLException::class)
    fun active(stmt: Statement?): Boolean

    fun checkIfExists(key: String?): Boolean
    interface Provider {
        @Throws(SQLException::class, IOException::class)
        fun provide(): ResultSet?
    }

    @Throws(SQLException::class)
    operator fun <T> get(statement: CachedStatement<*>?, key: String?, resultSetProvider: Provider?): ResultSet?

    fun supportsExpiration(): Boolean
}