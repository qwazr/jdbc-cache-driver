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
 */
package com.statewidesoftware.jdbc.cache

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

internal open class CachedPreparedStatement<T : PreparedStatement?> @JvmOverloads constructor(
    connection: CachedConnection?, resultSetCache: ResultSetCache?,
    backendStatement: T, sql: String?, resultSetConcurrency: Int = 0, resultSetType: Int = 0,
    resultSetHoldability: Int = 0
) : CachedStatement<T>(
    connection,
    resultSetCache,
    backendStatement,
    resultSetConcurrency,
    resultSetType,
    resultSetHoldability
), PreparedStatement {
    val parameters: SortedMap<Int, Any>

    init {
        parameters = TreeMap()
        executedSql = sql
    }

    @Throws(SQLException::class)
    override fun generateKey() {
        val sb = StringBuilder(executedSql)
        parameters.forEach { (index: Int?, value: Any) ->
            sb.append('•')
            sb.append(index)
            sb.append(value.toString())
        }
        generatedKey = generateCacheKey(sb.toString())
    }

    @Throws(SQLException::class)
    override fun executeQuery(): ResultSet? {
        generateKey()
        return resultSetCache?.get<Statement>(
            this,
            generatedKey,
            if (backendStatement != null) ResultSetCache.Provider { backendStatement.executeQuery() } else null)
    }

    @Throws(SQLException::class)
    override fun executeUpdate(): Int {
        return backendStatement?.executeUpdate() ?: throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setNull(parameterIndex: Int, sqlType: Int) {
        backendStatement?.setNull(parameterIndex, sqlType)
        parameters.remove(parameterIndex)
    }

    @Throws(SQLException::class)
    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        backendStatement?.setBoolean(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setByte(parameterIndex: Int, x: Byte) {
        backendStatement?.setByte(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setShort(parameterIndex: Int, x: Short) {
        backendStatement?.setShort(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setInt(parameterIndex: Int, x: Int) {
        backendStatement?.setInt(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setLong(parameterIndex: Int, x: Long) {
        backendStatement?.setLong(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setFloat(parameterIndex: Int, x: Float) {
        backendStatement?.setFloat(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setDouble(parameterIndex: Int, x: Double) {
        backendStatement?.setDouble(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal) {
        backendStatement?.setBigDecimal(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setString(parameterIndex: Int, x: String) {
        backendStatement?.setString(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setBytes(parameterIndex: Int, x: ByteArray) {
        backendStatement?.setBytes(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setDate(parameterIndex: Int, x: Date) {
        backendStatement?.setDate(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setTime(parameterIndex: Int, x: Time) {
        backendStatement?.setTime(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setTimestamp(parameterIndex: Int, x: Timestamp) {
        checkBackendStatement()!!.setTimestamp(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterIndex: Int, x: InputStream, length: Int) {
        checkBackendStatement()!!.setAsciiStream(parameterIndex, x)
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int) {
        checkBackendStatement()!!.setUnicodeStream(parameterIndex, x, length)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterIndex: Int, x: InputStream, length: Int) {
        checkBackendStatement()!!.setBinaryStream(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun clearParameters() {
        backendStatement?.clearParameters()
        parameters.clear()
    }

    @Throws(SQLException::class)
    override fun setObject(parameterIndex: Int, x: Any, targetSqlType: Int) {
        backendStatement?.setObject(parameterIndex, x, targetSqlType)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setObject(parameterIndex: Int, x: Any) {
        backendStatement?.setObject(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun execute(): Boolean {
        generateKey()
        return resultSetCache?.checkIfExists(generatedKey) == true || checkBackendStatement("No cache entry")!!.execute()
    }

    @Throws(SQLException::class)
    override fun addBatch() {
        checkBackendStatement()!!.addBatch()
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterIndex: Int, reader: Reader, length: Int) {
        checkBackendStatement()!!.setCharacterStream(parameterIndex, reader, length)
    }

    @Throws(SQLException::class)
    override fun setRef(parameterIndex: Int, x: Ref) {
        checkBackendStatement()!!.setRef(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterIndex: Int, x: Blob) {
        checkBackendStatement()!!.setBlob(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterIndex: Int, x: Clob) {
        checkBackendStatement()!!.setClob(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setArray(parameterIndex: Int, x: Array) {
        checkBackendStatement()!!.setArray(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun getMetaData(): ResultSetMetaData {
        return checkBackendStatement()!!.metaData
    }

    @Throws(SQLException::class)
    override fun setDate(parameterIndex: Int, x: Date, cal: Calendar) {
        backendStatement?.setDate(parameterIndex, x, cal)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setTime(parameterIndex: Int, x: Time, cal: Calendar) {
        backendStatement?.setTime(parameterIndex, x, cal)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar) {
        backendStatement?.setTimestamp(parameterIndex, x, cal)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String) {
        backendStatement?.setNull(parameterIndex, sqlType, typeName)
        parameters.remove(parameterIndex)
    }

    @Throws(SQLException::class)
    override fun setURL(parameterIndex: Int, x: URL) {
        backendStatement?.setURL(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun getParameterMetaData(): ParameterMetaData {
        return checkBackendStatement()!!.parameterMetaData
    }

    @Throws(SQLException::class)
    override fun setRowId(parameterIndex: Int, x: RowId) {
        backendStatement?.setRowId(parameterIndex, x)
        parameters[parameterIndex] = x
    }

    @Throws(SQLException::class)
    override fun setNString(parameterIndex: Int, value: String) {
        backendStatement?.setNString(parameterIndex, value)
        parameters[parameterIndex] = value
    }

    @Throws(SQLException::class)
    override fun setNCharacterStream(parameterIndex: Int, value: Reader, length: Long) {
        checkBackendStatement()!!.setNCharacterStream(parameterIndex, value, length)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterIndex: Int, value: NClob) {
        checkBackendStatement()!!.setNClob(parameterIndex, value)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterIndex: Int, reader: Reader, length: Long) {
        checkBackendStatement()!!.setClob(parameterIndex, reader, length)
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterIndex: Int, inputStream: InputStream, length: Long) {
        checkBackendStatement()!!.setBlob(parameterIndex, inputStream, length)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterIndex: Int, reader: Reader, length: Long) {
        checkBackendStatement()!!.setNClob(parameterIndex, reader, length)
    }

    @Throws(SQLException::class)
    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML) {
        checkBackendStatement()!!.setSQLXML(parameterIndex, xmlObject)
    }

    @Throws(SQLException::class)
    override fun setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int) {
        checkBackendStatement()!!.setObject(parameterIndex, targetSqlType, scaleOrLength)
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterIndex: Int, x: InputStream, length: Long) {
        checkBackendStatement()!!.setAsciiStream(parameterIndex, x, length)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterIndex: Int, x: InputStream, length: Long) {
        checkBackendStatement()!!.setBinaryStream(parameterIndex, x, length)
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterIndex: Int, reader: Reader, length: Long) {
        checkBackendStatement()!!.setCharacterStream(parameterIndex, reader, length)
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterIndex: Int, x: InputStream) {
        checkBackendStatement()!!.setAsciiStream(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterIndex: Int, x: InputStream) {
        checkBackendStatement()!!.setBinaryStream(parameterIndex, x)
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterIndex: Int, reader: Reader) {
        checkBackendStatement()!!.setCharacterStream(parameterIndex, reader)
    }

    @Throws(SQLException::class)
    override fun setNCharacterStream(parameterIndex: Int, value: Reader) {
        checkBackendStatement()!!.setNCharacterStream(parameterIndex, value)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterIndex: Int, reader: Reader) {
        checkBackendStatement()!!.setClob(parameterIndex, reader)
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterIndex: Int, inputStream: InputStream) {
        checkBackendStatement()!!.setBlob(parameterIndex, inputStream)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterIndex: Int, reader: Reader) {
        checkBackendStatement()!!.setNClob(parameterIndex, reader)
    }
}