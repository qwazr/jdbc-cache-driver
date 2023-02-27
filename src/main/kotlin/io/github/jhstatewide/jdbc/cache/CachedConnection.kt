/*
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
 */package io.github.jhstatewide.jdbc.cache

import java.sql.*
import java.util.*
import java.util.concurrent.Executor

class CachedConnection(private val connection: Connection?, val resultSetCache: io.github.jhstatewide.jdbc.cache.ResultSetCache?) : Connection {
    @Volatile
    private var autocommit = false

    @Volatile
    private var closed = false

    @Volatile
    private var readOnly = false

    @Volatile
    private var catalog: String? = null

    @Volatile
    private var transactionIsolation: Int

    @Volatile
    private var typeMap: Map<String, Class<*>>? = null

    @Volatile
    private var holdability = 0
    private val clientInfos: Properties

    @Volatile
    private var schema: String? = null

    init {
        transactionIsolation = Connection.TRANSACTION_NONE
        clientInfos = Properties()
    }

    @Throws(SQLException::class)
    override fun createStatement(): Statement {
        val statement = connection?.createStatement()
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedStatement(
            this,
            resultSetCache,
            statement
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String): PreparedStatement {
        val statement = connection?.prepareStatement(sql)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this,
            resultSetCache,
            statement,
            sql
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareCall(sql: String): CallableStatement {
        val statement = connection?.prepareCall(sql)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedCallableStatement(
            this,
            resultSetCache,
            statement,
            sql
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun nativeSQL(sql: String): String {
        return if (connection != null) connection.nativeSQL(sql) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setAutoCommit(autoCommit: Boolean) {
        if (connection != null) connection.autoCommit = autoCommit
        autocommit = autoCommit
    }

    @Throws(SQLException::class)
    override fun getAutoCommit(): Boolean {
        return connection?.autoCommit ?: autocommit
    }

    @Throws(SQLException::class)
    override fun commit() {
        if (connection != null) connection.commit() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rollback() {
        if (connection != null) connection.rollback() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun close() {
        connection?.close()
        closed = true
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return connection?.isClosed ?: closed
    }

    @Throws(SQLException::class)
    override fun getMetaData(): DatabaseMetaData {
        return if (connection != null) connection.metaData else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setReadOnly(readOnly: Boolean) {
        if (connection != null) connection.isReadOnly = readOnly
        this.readOnly = readOnly
    }

    @Throws(SQLException::class)
    override fun isReadOnly(): Boolean {
        return connection?.isReadOnly ?: readOnly
    }

    @Throws(SQLException::class)
    override fun setCatalog(catalog: String) {
        if (connection != null) connection.catalog = catalog
        this.catalog = catalog
    }

    @Throws(SQLException::class)
    override fun getCatalog(): String {
        return if (connection != null) connection.catalog else catalog!!
    }

    @Throws(SQLException::class)
    override fun setTransactionIsolation(level: Int) {
        if (connection != null) connection.transactionIsolation = level
        transactionIsolation = level
    }

    @Throws(SQLException::class)
    override fun getTransactionIsolation(): Int {
        return transactionIsolation
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning? {
        return connection?.warnings
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {
        connection?.clearWarnings()
    }

    @Throws(SQLException::class)
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        val statement = connection?.createStatement(resultSetType, resultSetConcurrency)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedStatement(
            this,
            resultSetCache,
            statement,
            resultSetType,
            resultSetConcurrency,
            0
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        val statement = connection?.prepareStatement(sql, resultSetType, resultSetConcurrency)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
            0
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        val statement = connection?.prepareCall(sql, resultSetType, resultSetConcurrency)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedCallableStatement(
            this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
            0
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun getTypeMap(): Map<String, Class<*>> {
        return if (connection != null) connection.typeMap else typeMap!!
    }

    @Throws(SQLException::class)
    override fun setTypeMap(map: Map<String, Class<*>>?) {
        if (connection != null) connection.typeMap = map
        typeMap = map
    }

    @Throws(SQLException::class)
    override fun setHoldability(holdability: Int) {
        if (connection != null) connection.holdability = holdability
        this.holdability = holdability
    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        return connection?.holdability ?: holdability
    }

    @Throws(SQLException::class)
    override fun setSavepoint(): Savepoint {
        return if (connection != null) connection.setSavepoint() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setSavepoint(name: String): Savepoint {
        return if (connection != null) connection.setSavepoint(name) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rollback(savepoint: Savepoint) {
        if (connection != null) connection.rollback(savepoint) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun releaseSavepoint(savepoint: Savepoint) {
        if (connection != null) connection.releaseSavepoint(savepoint) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        val statement = connection?.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedStatement(
            this, resultSetCache, statement, resultSetType, resultSetConcurrency,
            resultSetHoldability
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String, resultSetType: Int, resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        val statement = connection?.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
            resultSetHoldability
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareCall(
        sql: String, resultSetType: Int, resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        val statement = connection?.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedCallableStatement(
            this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
            resultSetHoldability
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement {
        val statement = connection?.prepareStatement(sql, autoGeneratedKeys)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this,
            resultSetCache,
            statement,
            sql
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String, columnIndexes: IntArray): PreparedStatement {
        val statement = connection?.prepareStatement(sql, columnIndexes)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this,
            resultSetCache,
            statement,
            sql
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String, columnNames: Array<String>): PreparedStatement {
        val statement = connection?.prepareStatement(sql, columnNames)
        return if (resultSetCache != null) io.github.jhstatewide.jdbc.cache.CachedPreparedStatement(
            this,
            resultSetCache,
            statement,
            sql
        ) else statement!!
    }

    @Throws(SQLException::class)
    override fun createClob(): Clob {
        return if (connection != null) connection.createClob() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createBlob(): Blob {
        return if (connection != null) connection.createBlob() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createNClob(): NClob {
        return if (connection != null) connection.createNClob() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createSQLXML(): SQLXML {
        return if (connection != null) connection.createSQLXML() else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isValid(timeout: Int): Boolean {
        return connection == null || connection.isValid(timeout)
    }

    @Throws(SQLClientInfoException::class)
    override fun setClientInfo(name: String, value: String) {
        synchronized(clientInfos) { clientInfos.put(name, value) }
    }

    @Throws(SQLClientInfoException::class)
    override fun setClientInfo(properties: Properties) {
        synchronized(clientInfos) {
            clientInfos.clear()
            clientInfos.putAll(properties)
        }
    }

    @Throws(SQLException::class)
    override fun getClientInfo(name: String): String {
        synchronized(clientInfos) { return clientInfos.getProperty(name) }
    }

    @Throws(SQLException::class)
    override fun getClientInfo(): Properties {
        synchronized(clientInfos) { return Properties(clientInfos) }
    }

    @Throws(SQLException::class)
    override fun createArrayOf(typeName: String, elements: Array<Any>): java.sql.Array {
        return if (connection != null) connection.createArrayOf(
            typeName,
            elements
        ) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createStruct(typeName: String, attributes: Array<Any>): Struct {
        return if (connection != null) connection.createStruct(
            typeName,
            attributes
        ) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setSchema(schema: String) {
        if (connection != null) connection.schema = schema
        this.schema = schema
    }

    @Throws(SQLException::class)
    override fun getSchema(): String? {
        return schema
    }

    @Throws(SQLException::class)
    override fun abort(executor: Executor) {
        if (connection != null) connection.abort(executor) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setNetworkTimeout(executor: Executor, milliseconds: Int) {
        if (connection != null) connection.setNetworkTimeout(
            executor,
            milliseconds
        ) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNetworkTimeout(): Int {
        return connection?.networkTimeout ?: throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        return if (connection != null) connection.unwrap(iface) else throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        return connection?.isWrapperFor(iface) ?: throw SQLFeatureNotSupportedException()
    }
}