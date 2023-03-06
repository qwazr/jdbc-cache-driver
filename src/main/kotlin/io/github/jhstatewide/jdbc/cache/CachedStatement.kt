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

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.sql.*

internal const val JDBC_CACHE_EXTENSION = ".jdbc_cache"

open class CachedStatement<T : Statement?> @JvmOverloads constructor(
    private val connection: CachedConnection?, val resultSetCache: ResultSetCache?,
    backendStatement: T, resultSetConcurrency: Int = 0, resultSetType: Int = 0,
    resultSetHoldability: Int = 0
) : Statement {
    val backendStatement: T?
    private val resultSetConcurrency: Int
    private val resultSetType: Int
    private val resultSetHoldability: Int

    @Volatile
    private var maxFieldSize: Int

    @Volatile
    private var maxRows: Int

    @Volatile
    private var queryTimeOut: Int

    @Volatile
    private var fetchDirection: Int

    @Volatile
    private var fetchSize: Int

    @Volatile
    private var closed: Boolean

    @Volatile
    private var poolable: Boolean

    @Volatile
    private var closeOnCompletion: Boolean

    @Volatile
    var executedSql: String?

    @Volatile
    var generatedKey: String? = null

    init {
        this.backendStatement = backendStatement
        this.resultSetConcurrency = resultSetConcurrency
        this.resultSetType = resultSetType
        this.resultSetHoldability = resultSetHoldability
        maxFieldSize = 0
        maxRows = 0
        queryTimeOut = 0
        fetchDirection = 0
        fetchSize = 0
        closed = false
        poolable = false
        closeOnCompletion = false
        executedSql = null
    }

    @JvmOverloads
    @Throws(SQLException::class)
    fun checkBackendStatement(error: String? = null): T {
        return backendStatement ?: throw error?.let { SQLException(it) } ?: SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    protected open fun generateKey() {
        generatedKey = generateCacheKey(executedSql)
    }

    @get:Throws(SQLException::class)
    val orGenerateKey: String?
        get() {
            if (generatedKey == null) generateKey()
            return generatedKey
        }

    @Throws(SQLException::class)
    override fun executeQuery(sql: String): ResultSet? {
        executedSql = sql
        generateKey()
        return resultSetCache
            ?.get<Statement>(
                this,
                generatedKey,
                if (backendStatement == null) {
                    null
                } else {
                    object : ResultSetCache.Provider {
                        override fun provide(): ResultSet? {
                            return backendStatement.executeQuery(sql)
                        }
                    }
                }
            )
    }

    @Throws(SQLException::class)
    override fun executeUpdate(sql: String): Int {
        executedSql = sql
        return checkBackendStatement()!!.executeUpdate(sql)
    }

    @Throws(SQLException::class)
    override fun close() {
        backendStatement?.close()
        closed = true
    }

    @Throws(SQLException::class)
    override fun getMaxFieldSize(): Int {
        return backendStatement?.maxFieldSize ?: maxFieldSize
    }

    @Throws(SQLException::class)
    override fun setMaxFieldSize(max: Int) {
        if (backendStatement != null) backendStatement.maxFieldSize = max
        maxFieldSize = max
    }

    @Throws(SQLException::class)
    override fun getMaxRows(): Int {
        return backendStatement?.maxRows ?: maxRows
    }

    @Throws(SQLException::class)
    override fun setMaxRows(max: Int) {
        if (backendStatement != null) backendStatement.maxRows = max
        maxRows = max
    }

    @Throws(SQLException::class)
    override fun setEscapeProcessing(enable: Boolean) {
        backendStatement?.setEscapeProcessing(enable)
    }

    @Throws(SQLException::class)
    override fun getQueryTimeout(): Int {
        return backendStatement?.queryTimeout ?: queryTimeOut
    }

    @Throws(SQLException::class)
    override fun setQueryTimeout(seconds: Int) {
        if (backendStatement != null) backendStatement.queryTimeout = seconds
        queryTimeOut = seconds
    }

    @Throws(SQLException::class)
    override fun cancel() {
        backendStatement?.cancel()
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning? {
        return backendStatement?.warnings
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {
        backendStatement?.clearWarnings()
    }

    @Throws(SQLException::class)
    override fun setCursorName(name: String) {
        backendStatement?.setCursorName(name)
    }

    @Throws(SQLException::class)
    override fun execute(sql: String): Boolean {
        executedSql = sql
        generateKey()
        return resultSetCache?.checkIfExists(generatedKey) == true || checkBackendStatement("No cache entry")!!.execute(sql)
    }

    @Throws(SQLException::class)
    override fun getResultSet(): ResultSet? {
        generateKey()
        return resultSetCache?.get<Statement>(
            this,
            generatedKey,
            if (backendStatement == null) {
                null
            } else {
                object : ResultSetCache.Provider {
                    override fun provide(): ResultSet? {
                        return backendStatement.resultSet
                    }
                }
            }
        )
    }

    @Throws(SQLException::class)
    override fun getUpdateCount(): Int {
        return checkBackendStatement()!!.updateCount
    }

    @Throws(SQLException::class)
    override fun getMoreResults(): Boolean {
        return checkBackendStatement()!!.moreResults
    }

    @Throws(SQLException::class)
    override fun setFetchDirection(direction: Int) {
        if (backendStatement != null) backendStatement.fetchDirection = direction
        fetchDirection = direction
    }

    @Throws(SQLException::class)
    override fun getFetchDirection(): Int {
        return backendStatement?.fetchDirection ?: fetchDirection
    }

    @Throws(SQLException::class)
    override fun setFetchSize(rows: Int) {
        if (backendStatement != null) backendStatement.fetchSize = rows
        fetchSize = rows
    }

    @Throws(SQLException::class)
    override fun getFetchSize(): Int {
        return backendStatement?.fetchSize ?: fetchSize
    }

    @Throws(SQLException::class)
    override fun getResultSetConcurrency(): Int {
        return resultSetConcurrency
    }

    @Throws(SQLException::class)
    override fun getResultSetType(): Int {
        return resultSetType
    }

    @Throws(SQLException::class)
    override fun addBatch(sql: String) {
        checkBackendStatement()!!.addBatch(sql)
    }

    @Throws(SQLException::class)
    override fun clearBatch() {
        checkBackendStatement()!!.clearBatch()
    }

    @Throws(SQLException::class)
    override fun executeBatch(): IntArray {
        return checkBackendStatement()!!.executeBatch()
    }

    @Throws(SQLException::class)
    override fun getConnection(): CachedConnection? {
        return connection
    }

    @Throws(SQLException::class)
    override fun getMoreResults(current: Int): Boolean {
        return checkBackendStatement()!!.getMoreResults(current)
    }

    @Throws(SQLException::class)
    override fun getGeneratedKeys(): ResultSet {
        return checkBackendStatement()!!.generatedKeys
    }

    @Throws(SQLException::class)
    override fun executeUpdate(sql: String, autoGeneratedKeys: Int): Int {
        executedSql = sql
        return checkBackendStatement()!!.executeUpdate(sql, autoGeneratedKeys)
    }

    @Throws(SQLException::class)
    override fun executeUpdate(sql: String, columnIndexes: IntArray): Int {
        executedSql = sql
        return checkBackendStatement()!!.executeUpdate(sql, columnIndexes)
    }

    @Throws(SQLException::class)
    override fun executeUpdate(sql: String, columnNames: Array<String>): Int {
        executedSql = sql
        return checkBackendStatement()!!.executeUpdate(sql, columnNames)
    }

    @Throws(SQLException::class)
    override fun execute(sql: String, autoGeneratedKeys: Int): Boolean {
        executedSql = sql
        generateKey()
        return resultSetCache?.checkIfExists(generatedKey) == true || checkBackendStatement("No cache entry")
            ?.execute(sql, autoGeneratedKeys) ?: false
    }

    @Throws(SQLException::class)
    override fun execute(sql: String, columnIndexes: IntArray): Boolean {
        executedSql = sql
        generateKey()
        return resultSetCache?.checkIfExists(generatedKey) == true || checkBackendStatement("No cache entry")
            ?.execute(sql, columnIndexes) ?: false
    }

    @Throws(SQLException::class)
    override fun execute(sql: String, columnNames: Array<String>): Boolean {
        executedSql = sql
        generateKey()
        return resultSetCache?.checkIfExists(generatedKey) == true || checkBackendStatement("No cache entry")
            ?.execute(sql, columnNames) ?: false
    }

    @Throws(SQLException::class)
    override fun getResultSetHoldability(): Int {
        return resultSetHoldability
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return backendStatement?.isClosed ?: closed
    }

    @Throws(SQLException::class)
    override fun setPoolable(poolable: Boolean) {
        if (backendStatement != null) backendStatement.isPoolable = poolable
        this.poolable = poolable
    }

    @Throws(SQLException::class)
    override fun isPoolable(): Boolean {
        return backendStatement?.isPoolable ?: poolable
    }

    @Throws(SQLException::class)
    override fun closeOnCompletion() {
        backendStatement?.closeOnCompletion()
        closeOnCompletion = true
    }

    @Throws(SQLException::class)
    override fun isCloseOnCompletion(): Boolean {
        return backendStatement?.isCloseOnCompletion ?: closeOnCompletion
    }

    @Throws(SQLException::class)
    override fun <V> unwrap(iface: Class<V>): V {
        return checkBackendStatement()!!.unwrap(iface)
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        return checkBackendStatement()!!.isWrapperFor(iface)
    }

    companion object {
        @Throws(SQLException::class)
        fun generateCacheKey(src: String?): String {
            return try {
                val md = MessageDigest.getInstance("MD5")
                DatatypeConverter.printHexBinary(md.digest(src!!.toByteArray())) + JDBC_CACHE_EXTENSION
            } catch (e: NoSuchAlgorithmException) {
                throw SQLException("MD5 is not available")
            }
        }
    }
}