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
 */package com.statewidesoftware.jdbc.cache

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

internal class CachedCallableStatement @JvmOverloads constructor(
    connection: CachedConnection?, resultSetCache: ResultSetCache?,
    backendStatement: CallableStatement?, sql: String?, resultSetConcurrency: Int = 0,
    resultSetType: Int = 0, resultSetHoldability: Int = 0
) : CachedPreparedStatement<CallableStatement?>(
    connection, resultSetCache, backendStatement, sql, resultSetConcurrency, resultSetType,
    resultSetHoldability
), CallableStatement {
    private val namedParameters: SortedMap<String, Any>

    init {
        namedParameters = TreeMap()
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterIndex: Int, sqlType: Int) {
        checkBackendStatement()!!.registerOutParameter(parameterIndex, sqlType)
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterIndex: Int, sqlType: Int, scale: Int) {
        checkBackendStatement()!!.registerOutParameter(parameterIndex, sqlType, scale)
    }

    @Throws(SQLException::class)
    override fun wasNull(): Boolean {
        return checkBackendStatement()!!.wasNull()
    }

    @Throws(SQLException::class)
    override fun getString(parameterIndex: Int): String {
        return if (backendStatement != null) backendStatement.getString(parameterIndex) else parameters[parameterIndex] as String
    }

    @Throws(SQLException::class)
    override fun getBoolean(parameterIndex: Int): Boolean {
        return backendStatement?.getBoolean(parameterIndex) ?: parameters[parameterIndex] as Boolean
    }

    @Throws(SQLException::class)
    override fun getByte(parameterIndex: Int): Byte {
        return backendStatement?.getByte(parameterIndex) ?: parameters[parameterIndex] as Byte
    }

    @Throws(SQLException::class)
    override fun getShort(parameterIndex: Int): Short {
        return backendStatement?.getShort(parameterIndex) ?: parameters[parameterIndex] as Short
    }

    @Throws(SQLException::class)
    override fun getInt(parameterIndex: Int): Int {
        return backendStatement?.getInt(parameterIndex) ?: parameters[parameterIndex] as Int
    }

    @Throws(SQLException::class)
    override fun getLong(parameterIndex: Int): Long {
        return backendStatement?.getLong(parameterIndex) ?: parameters[parameterIndex] as Long
    }

    @Throws(SQLException::class)
    override fun getFloat(parameterIndex: Int): Float {
        return backendStatement?.getFloat(parameterIndex) ?: parameters[parameterIndex] as Float
    }

    @Throws(SQLException::class)
    override fun getDouble(parameterIndex: Int): Double {
        return backendStatement?.getDouble(parameterIndex) ?: parameters[parameterIndex] as Double
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun getBigDecimal(parameterIndex: Int, scale: Int): BigDecimal {
        return if (backendStatement != null) backendStatement.getBigDecimal(
            parameterIndex,
            scale
        ) else parameters[parameterIndex] as BigDecimal
    }

    @Throws(SQLException::class)
    override fun getBytes(parameterIndex: Int): ByteArray {
        return if (backendStatement != null) backendStatement.getBytes(parameterIndex) else parameters[parameterIndex] as ByteArray
    }

    @Throws(SQLException::class)
    override fun getDate(parameterIndex: Int): Date {
        return if (backendStatement != null) backendStatement.getDate(parameterIndex) else parameters[parameterIndex] as Date
    }

    @Throws(SQLException::class)
    override fun getTime(parameterIndex: Int): Time {
        return if (backendStatement != null) backendStatement.getTime(parameterIndex) else parameters[parameterIndex] as Time
    }

    @Throws(SQLException::class)
    override fun getTimestamp(parameterIndex: Int): Timestamp {
        return if (backendStatement != null) backendStatement.getTimestamp(parameterIndex) else parameters[parameterIndex] as Timestamp
    }

    @Throws(SQLException::class)
    override fun getObject(parameterIndex: Int): Any {
        return if (backendStatement != null) backendStatement.getObject(parameterIndex) else parameters[parameterIndex]!!
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(parameterIndex: Int): BigDecimal {
        return if (backendStatement != null) backendStatement.getBigDecimal(parameterIndex) else parameters[parameterIndex] as BigDecimal
    }

    @Throws(SQLException::class)
    override fun getObject(parameterIndex: Int, map: Map<String?, Class<*>?>?): Any {
        return if (backendStatement != null) backendStatement.getObject(
            parameterIndex,
            map
        ) else parameters[parameterIndex]!!
    }

    @Throws(SQLException::class)
    override fun getRef(parameterIndex: Int): Ref {
        return if (backendStatement != null) backendStatement.getRef(parameterIndex) else parameters[parameterIndex] as Ref
    }

    @Throws(SQLException::class)
    override fun getBlob(parameterIndex: Int): Blob {
        return if (backendStatement != null) backendStatement.getBlob(parameterIndex) else parameters[parameterIndex] as Blob
    }

    @Throws(SQLException::class)
    override fun getClob(parameterIndex: Int): Clob {
        return if (backendStatement != null) backendStatement.getClob(parameterIndex) else parameters[parameterIndex] as Clob
    }

    @Throws(SQLException::class)
    override fun getArray(parameterIndex: Int): Array {
        return if (backendStatement != null) backendStatement.getArray(parameterIndex) else parameters[parameterIndex] as Array
    }

    @Throws(SQLException::class)
    override fun getDate(parameterIndex: Int, cal: Calendar): Date {
        return if (backendStatement != null) backendStatement.getDate(parameterIndex) else parameters[parameterIndex] as Date
    }

    @Throws(SQLException::class)
    override fun getTime(parameterIndex: Int, cal: Calendar): Time {
        return if (backendStatement != null) backendStatement.getTime(
            parameterIndex,
            cal
        ) else parameters[parameterIndex] as Time
    }

    @Throws(SQLException::class)
    override fun getTimestamp(parameterIndex: Int, cal: Calendar): Timestamp {
        return if (backendStatement != null) backendStatement.getTimestamp(
            parameterIndex,
            cal
        ) else parameters[parameterIndex] as Timestamp
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterIndex: Int, sqlType: Int, typeName: String) {
        backendStatement?.registerOutParameter(parameterIndex, sqlType, typeName)
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterName: String, sqlType: Int) {
        backendStatement?.registerOutParameter(parameterName, sqlType)
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterName: String, sqlType: Int, scale: Int) {
        backendStatement?.registerOutParameter(parameterName, sqlType, scale)
    }

    @Throws(SQLException::class)
    override fun registerOutParameter(parameterName: String, sqlType: Int, typeName: String) {
        backendStatement?.registerOutParameter(parameterName, sqlType, typeName)
    }

    @Throws(SQLException::class)
    override fun getURL(parameterIndex: Int): URL {
        return if (backendStatement != null) backendStatement.getURL(parameterIndex) else parameters[parameterIndex] as URL
    }

    @Throws(SQLException::class)
    override fun setURL(parameterName: String, `val`: URL) {
        backendStatement?.setURL(parameterName, `val`)
        namedParameters[parameterName] = `val`
    }

    @Throws(SQLException::class)
    override fun setNull(parameterName: String, sqlType: Int) {
        backendStatement?.setNull(parameterName, sqlType)
        namedParameters.remove(parameterName)
    }

    @Throws(SQLException::class)
    override fun setBoolean(parameterName: String, x: Boolean) {
        backendStatement?.setBoolean(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setByte(parameterName: String, x: Byte) {
        backendStatement?.setByte(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setShort(parameterName: String, x: Short) {
        backendStatement?.setShort(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setInt(parameterName: String, x: Int) {
        backendStatement?.setInt(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setLong(parameterName: String, x: Long) {
        backendStatement?.setLong(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setFloat(parameterName: String, x: Float) {
        backendStatement?.setFloat(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setDouble(parameterName: String, x: Double) {
        backendStatement?.setDouble(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setBigDecimal(parameterName: String, x: BigDecimal) {
        backendStatement?.setBigDecimal(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setString(parameterName: String, x: String) {
        backendStatement?.setString(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setBytes(parameterName: String, x: ByteArray) {
        backendStatement?.setBytes(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setDate(parameterName: String, x: Date) {
        backendStatement?.setDate(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setTime(parameterName: String, x: Time) {
        backendStatement?.setTime(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setTimestamp(parameterName: String, x: Timestamp) {
        backendStatement?.setTimestamp(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterName: String, x: InputStream, length: Int) {
        checkBackendStatement()!!.setAsciiStream(parameterName, x, length)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterName: String, x: InputStream, length: Int) {
        checkBackendStatement()!!.setBinaryStream(parameterName, x, length)
    }

    @Throws(SQLException::class)
    override fun setObject(parameterName: String, x: Any, targetSqlType: Int, scale: Int) {
        backendStatement?.setObject(parameterName, x, targetSqlType, scale)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setObject(parameterName: String, x: Any, targetSqlType: Int) {
        backendStatement?.setObject(parameterName, x, targetSqlType)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setObject(parameterName: String, x: Any) {
        backendStatement?.setObject(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterName: String, reader: Reader, length: Int) {
        checkBackendStatement()!!.setCharacterStream(parameterName, reader, length)
    }

    @Throws(SQLException::class)
    override fun setDate(parameterName: String, x: Date, cal: Calendar) {
        backendStatement?.setDate(parameterName, x, cal)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setTime(parameterName: String, x: Time, cal: Calendar) {
        backendStatement?.setTime(parameterName, x, cal)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setTimestamp(parameterName: String, x: Timestamp, cal: Calendar) {
        backendStatement?.setTimestamp(parameterName, x, cal)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setNull(parameterName: String, sqlType: Int, typeName: String) {
        backendStatement?.setNull(parameterName, sqlType, typeName)
        namedParameters.remove(parameterName)
    }

    @Throws(SQLException::class)
    override fun getString(parameterName: String): String {
        return if (backendStatement != null) backendStatement.getString(parameterName) else namedParameters[parameterName] as String
    }

    @Throws(SQLException::class)
    override fun getBoolean(parameterName: String): Boolean {
        return backendStatement?.getBoolean(parameterName) ?: namedParameters[parameterName] as Boolean
    }

    @Throws(SQLException::class)
    override fun getByte(parameterName: String): Byte {
        return backendStatement?.getByte(parameterName) ?: namedParameters[parameterName] as Byte
    }

    @Throws(SQLException::class)
    override fun getShort(parameterName: String): Short {
        return backendStatement?.getShort(parameterName) ?: namedParameters[parameterName] as Short
    }

    @Throws(SQLException::class)
    override fun getInt(parameterName: String): Int {
        return backendStatement?.getInt(parameterName) ?: namedParameters[parameterName] as Int
    }

    @Throws(SQLException::class)
    override fun getLong(parameterName: String): Long {
        return backendStatement?.getLong(parameterName) ?: namedParameters[parameterName] as Long
    }

    @Throws(SQLException::class)
    override fun getFloat(parameterName: String): Float {
        return backendStatement?.getFloat(parameterName) ?: namedParameters[parameterName] as Float
    }

    @Throws(SQLException::class)
    override fun getDouble(parameterName: String): Double {
        return backendStatement?.getDouble(parameterName) ?: namedParameters[parameterName] as Double
    }

    @Throws(SQLException::class)
    override fun getBytes(parameterName: String): ByteArray {
        return if (backendStatement != null) backendStatement.getBytes(parameterName) else namedParameters[parameterName] as ByteArray
    }

    @Throws(SQLException::class)
    override fun getDate(parameterName: String): Date {
        return if (backendStatement != null) backendStatement.getDate(parameterName) else namedParameters[parameterName] as Date
    }

    @Throws(SQLException::class)
    override fun getTime(parameterName: String): Time {
        return if (backendStatement != null) backendStatement.getTime(parameterName) else namedParameters[parameterName] as Time
    }

    @Throws(SQLException::class)
    override fun getTimestamp(parameterName: String): Timestamp {
        return if (backendStatement != null) backendStatement.getTimestamp(parameterName) else namedParameters[parameterName] as Timestamp
    }

    @Throws(SQLException::class)
    override fun getObject(parameterName: String): Any {
        return if (backendStatement != null) backendStatement.getObject(parameterName) else namedParameters[parameterName]!!
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(parameterName: String): BigDecimal {
        return if (backendStatement != null) backendStatement.getBigDecimal(parameterName) else namedParameters[parameterName] as BigDecimal
    }

    @Throws(SQLException::class)
    override fun getObject(parameterName: String, map: Map<String?, Class<*>?>?): Any {
        return if (backendStatement != null) backendStatement.getObject(
            parameterName,
            map
        ) else namedParameters[parameterName]!!
    }

    @Throws(SQLException::class)
    override fun getRef(parameterName: String): Ref {
        return if (backendStatement != null) backendStatement.getRef(parameterName) else namedParameters[parameterName] as Ref
    }

    @Throws(SQLException::class)
    override fun getBlob(parameterName: String): Blob {
        return if (backendStatement != null) backendStatement.getBlob(parameterName) else namedParameters[parameterName] as Blob
    }

    @Throws(SQLException::class)
    override fun getClob(parameterName: String): Clob {
        return if (backendStatement != null) backendStatement.getClob(parameterName) else namedParameters[parameterName] as Clob
    }

    @Throws(SQLException::class)
    override fun getArray(parameterName: String): Array {
        return if (backendStatement != null) backendStatement.getArray(parameterName) else namedParameters[parameterName] as Array
    }

    @Throws(SQLException::class)
    override fun getDate(parameterName: String, cal: Calendar): Date {
        return if (backendStatement != null) backendStatement.getDate(parameterName) else namedParameters[parameterName] as Date
    }

    @Throws(SQLException::class)
    override fun getTime(parameterName: String, cal: Calendar): Time {
        return if (backendStatement != null) backendStatement.getTime(parameterName) else namedParameters[parameterName] as Time
    }

    @Throws(SQLException::class)
    override fun getTimestamp(parameterName: String, cal: Calendar): Timestamp {
        return if (backendStatement != null) backendStatement.getTimestamp(
            parameterName,
            cal
        ) else namedParameters[parameterName] as Timestamp
    }

    @Throws(SQLException::class)
    override fun getURL(parameterName: String): URL {
        return if (backendStatement != null) backendStatement.getURL(parameterName) else namedParameters[parameterName] as URL
    }

    @Throws(SQLException::class)
    override fun getRowId(parameterIndex: Int): RowId {
        return if (backendStatement != null) backendStatement.getRowId(parameterIndex) else parameters[parameterIndex] as RowId
    }

    @Throws(SQLException::class)
    override fun getRowId(parameterName: String): RowId {
        return if (backendStatement != null) backendStatement.getRowId(parameterName) else namedParameters[parameterName] as RowId
    }

    @Throws(SQLException::class)
    override fun setRowId(parameterName: String, x: RowId) {
        backendStatement?.setRowId(parameterName, x)
        namedParameters[parameterName] = x
    }

    @Throws(SQLException::class)
    override fun setNString(parameterName: String, value: String) {
        backendStatement?.setNString(parameterName, value)
        namedParameters[parameterName] = value
    }

    @Throws(SQLException::class)
    override fun setNCharacterStream(parameterName: String, value: Reader, length: Long) {
        checkBackendStatement()!!.setNCharacterStream(parameterName, value, length)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterName: String, value: NClob) {
        checkBackendStatement()!!.setNClob(parameterName, value)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterName: String, reader: Reader, length: Long) {
        checkBackendStatement()!!.setClob(parameterName, reader, length)
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterName: String, inputStream: InputStream, length: Long) {
        checkBackendStatement()!!.setBlob(parameterName, inputStream, length)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterName: String, reader: Reader, length: Long) {
        checkBackendStatement()!!.setNClob(parameterName, reader, length)
    }

    @Throws(SQLException::class)
    override fun getNClob(parameterIndex: Int): NClob {
        return if (backendStatement != null) backendStatement.getNClob(parameterIndex) else parameters[parameterIndex] as NClob
    }

    @Throws(SQLException::class)
    override fun getNClob(parameterName: String): NClob {
        return if (backendStatement != null) backendStatement.getNClob(parameterName) else namedParameters[parameterName] as NClob
    }

    @Throws(SQLException::class)
    override fun setSQLXML(parameterName: String, xmlObject: SQLXML) {
        checkBackendStatement()!!.setSQLXML(parameterName, xmlObject)
    }

    @Throws(SQLException::class)
    override fun getSQLXML(parameterIndex: Int): SQLXML {
        return if (backendStatement != null) backendStatement.getSQLXML(parameterIndex) else parameters[parameterIndex] as SQLXML
    }

    @Throws(SQLException::class)
    override fun getSQLXML(parameterName: String): SQLXML {
        return if (backendStatement != null) backendStatement.getSQLXML(parameterName) else namedParameters[parameterName] as SQLXML
    }

    @Throws(SQLException::class)
    override fun getNString(parameterIndex: Int): String {
        return if (backendStatement != null) backendStatement.getNString(parameterIndex) else parameters[parameterIndex] as String
    }

    @Throws(SQLException::class)
    override fun getNString(parameterName: String): String {
        return if (backendStatement != null) backendStatement.getNString(parameterName) else namedParameters[parameterName] as String
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(parameterIndex: Int): Reader {
        return if (backendStatement != null) backendStatement.getNCharacterStream(parameterIndex) else parameters[parameterIndex] as Reader
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(parameterName: String): Reader {
        return if (backendStatement != null) backendStatement.getNCharacterStream(parameterName) else namedParameters[parameterName] as Reader
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(parameterIndex: Int): Reader {
        return if (backendStatement != null) backendStatement.getCharacterStream(parameterIndex) else parameters[parameterIndex] as Reader
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(parameterName: String): Reader {
        return if (backendStatement != null) backendStatement.getCharacterStream(parameterName) else namedParameters[parameterName] as Reader
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterName: String, x: Blob) {
        checkBackendStatement()!!.setBlob(parameterName, x)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterName: String, x: Clob) {
        checkBackendStatement()!!.setClob(parameterName, x)
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterName: String, x: InputStream, length: Long) {
        checkBackendStatement()!!.setAsciiStream(parameterName, x, length)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterName: String, x: InputStream, length: Long) {
        checkBackendStatement()!!.setBinaryStream(parameterName, x, length)
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterName: String, reader: Reader, length: Long) {
        checkBackendStatement()!!.setCharacterStream(parameterName, reader, length)
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(parameterName: String, x: InputStream) {
        checkBackendStatement()!!.setAsciiStream(parameterName, x)
    }

    @Throws(SQLException::class)
    override fun setBinaryStream(parameterName: String, x: InputStream) {
        checkBackendStatement()!!.setBinaryStream(parameterName, x)
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(parameterName: String, reader: Reader) {
        checkBackendStatement()!!.setCharacterStream(parameterName, reader)
    }

    @Throws(SQLException::class)
    override fun setNCharacterStream(parameterName: String, value: Reader) {
        checkBackendStatement()!!.setNCharacterStream(parameterName, value)
    }

    @Throws(SQLException::class)
    override fun setClob(parameterName: String, reader: Reader) {
        checkBackendStatement()!!.setClob(parameterName, reader)
    }

    @Throws(SQLException::class)
    override fun setBlob(parameterName: String, inputStream: InputStream) {
        checkBackendStatement()!!.setBlob(parameterName, inputStream)
    }

    @Throws(SQLException::class)
    override fun setNClob(parameterName: String, reader: Reader) {
        checkBackendStatement()!!.setNClob(parameterName, reader)
    }

    @Throws(SQLException::class)
    override fun <T> getObject(parameterIndex: Int, type: Class<T>): T {
        return if (backendStatement != null) backendStatement.getObject(
            parameterIndex,
            type
        ) else type.cast(parameters[parameterIndex])
    }

    @Throws(SQLException::class)
    override fun <T> getObject(parameterName: String, type: Class<T>): T {
        return if (backendStatement != null) backendStatement.getObject(parameterName, type) else type.cast(
            namedParameters[parameterName]
        )
    }
}