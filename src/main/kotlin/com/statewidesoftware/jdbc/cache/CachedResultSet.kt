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

import com.statewidesoftware.jdbc.cache.ResultSetWriter.readColumns
import com.statewidesoftware.jdbc.cache.ResultSetWriter.readRow
import java.io.*
import java.math.BigDecimal
import java.net.MalformedURLException
import java.net.URL
import java.sql.*
import java.sql.Date
import java.text.DateFormat
import java.text.ParseException
import java.util.*

/**
 * Cached ResultSet
 */
internal abstract class CachedResultSet(private val statement: CachedStatement<*>, private val input: DataInputStream) :
    ResultSet {
    private var metaData: CachedResultSetMetaData
    private val currentRow: Array<Any?>
    private val nextRow: Array<Any?>
    private var columnNames: HashMap<String, Int>

    @Volatile
    private var wasNull = false

    @Volatile
    private var currentPos = 0

    @Volatile
    private var nextPos = 0

    @Volatile
    private var closed = false

    init {
        try {
            metaData = CachedResultSetMetaData(readColumns(input))
            currentRow = arrayOfNulls(metaData.columns.size)
            nextRow = arrayOfNulls(metaData.columns.size)
            columnNames = HashMap()
            var i = 0
            for (column in metaData.columns) columnNames[column.label] = ++i
            readNext()
        } catch (e: IOException) {
            try {
                close()
            } catch (ex: Exception) {
                //Close quietly
            }
            throw SQLException("Cannot read the cache for statement $statement", e)
        }
    }

    @Throws(SQLException::class)
    private fun readNext() {
        try {
            nextPos = input.readInt()
            if (nextPos != currentPos + 1) {
                throw SQLException("Expects pos " + (currentPos + 1) + ", but got: " + nextPos)
            }
        } catch (e: EOFException) {
            nextPos = 0
            return
        } catch (e: IOException) {
            throw SQLException(e)
        }
        var i = 0
        try {
            for (column in metaData.columns) nextRow[i++] = readRow(column.type, input)
        } catch (e: IOException) {
            throw SQLException("Cannot extract column $i - pos $nextPos", e)
        }
    }

    @Throws(SQLException::class)
    override fun next(): Boolean {
        currentPos = nextPos
        if (currentPos == 0) return false
        var i = 0
        for (col in nextRow) currentRow[i++] = col
        readNext()
        return true
    }

    @Throws(SQLException::class)
    override fun close() {
        closed = try {
            input.close()
            true
        } catch (e: IOException) {
            throw SQLException(e)
        }
    }

    @Throws(SQLException::class)
    override fun wasNull(): Boolean {
        return wasNull
    }

    @Throws(SQLException::class)
    private fun checkColumn(columnIndex: Int): Any? {
        if (columnIndex == 0 || columnIndex > currentRow.size) throw SQLException("Column out of bounds")
        val `val` = currentRow[columnIndex - 1]
        wasNull = `val` == null
        return `val`
    }

    @Throws(SQLException::class)
    override fun getString(columnIndex: Int): String? {
        val `val` = checkColumn(columnIndex)
        return `val`?.toString()
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnIndex: Int): Boolean {
        val `val` = checkColumn(columnIndex) ?: return false
        if (`val` is Boolean) return `val`
        return if (`val` is Number) `val`.toInt() == 0 else java.lang.Boolean.parseBoolean(
            `val`.toString()
        )
    }

    @Throws(SQLException::class)
    override fun getByte(columnIndex: Int): Byte {
        val `val` = checkColumn(columnIndex) ?: return 0
        if (`val` is Byte) return `val`
        return if (`val` is Number) `val`.toByte() else `val`.toString().toByte()
    }

    @Throws(SQLException::class)
    override fun getShort(columnIndex: Int): Short {
        val `val` = checkColumn(columnIndex) ?: return 0
        if (`val` is Short) return `val`
        return if (`val` is Number) `val`.toShort() else `val`.toString().toShort()
    }

    @Throws(SQLException::class)
    override fun getInt(columnIndex: Int): Int {
        val `val` = checkColumn(columnIndex) ?: return 0
        if (`val` is Int) return `val`
        return if (`val` is Number) `val`.toInt() else `val`.toString().toInt()
    }

    @Throws(SQLException::class)
    override fun getLong(columnIndex: Int): Long {
        val `val` = checkColumn(columnIndex) ?: return 0
        if (`val` is Long) return `val`
        return if (`val` is Number) `val`.toLong() else `val`.toString().toLong()
    }

    @Throws(SQLException::class)
    override fun getFloat(columnIndex: Int): Float {
        val `val` = checkColumn(columnIndex) ?: return 0f
        if (`val` is Float) return `val`
        return if (`val` is Number) `val`.toFloat() else `val`.toString().toFloat()
    }

    @Throws(SQLException::class)
    override fun getDouble(columnIndex: Int): Double {
        val `val` = checkColumn(columnIndex) ?: return 0.0
        if (`val` is Double) return `val`
        return if (`val` is Number) `val`.toDouble() else `val`.toString().toDouble()
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
        return getBigDecimal(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getBytes(columnIndex: Int): ByteArray? {
        val `val` = checkColumn(columnIndex) ?: return null
        return if (`val` is ByteArray) `val` else ByteArray(0)
    }

    @Throws(SQLException::class)
    override fun getDate(columnIndex: Int): Date? {
        val `val` = checkColumn(columnIndex) ?: return null
        if (`val` is Date) return `val`
        if (`val` is Timestamp) return Date(`val`.time)
        return if (`val` is Number) Date(`val`.toLong()) else try {
            Date(DateFormat.getDateInstance().parse(`val`.toString()).time)
        } catch (e: ParseException) {
            throw SQLException("Unexpected Date type (" + `val`.javaClass + ") on column " + columnIndex, e)
        }
    }

    @Throws(SQLException::class)
    override fun getTime(columnIndex: Int): Time? {
        val `val` = checkColumn(columnIndex) ?: return null
        if (`val` is Time) return `val`
        return if (`val` is Number) Time(`val`.toLong()) else try {
            Time(DateFormat.getTimeInstance().parse(`val`.toString()).time)
        } catch (e: ParseException) {
            throw SQLException("Unexpected Time type (" + `val`.javaClass + ") on column " + columnIndex, e)
        }
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnIndex: Int): Timestamp? {
        val `val` = checkColumn(columnIndex) ?: return null
        if (`val` is Timestamp) return `val`
        return if (`val` is Number) Timestamp(`val`.toLong()) else try {
            Timestamp(DateFormat.getDateTimeInstance().parse(`val`.toString()).time)
        } catch (e: ParseException) {
            throw SQLException("Unexpected Timestamp type (" + `val`.javaClass + ") on column " + columnIndex, e)
        }
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnIndex: Int): InputStream? {
        checkColumn(columnIndex)
        return null
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun getUnicodeStream(columnIndex: Int): InputStream? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnIndex: Int): InputStream? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    private fun checkColumn(label: String): Int {
        return columnNames[label] ?: throw SQLException("Column not found: $label")
    }

    @Throws(SQLException::class)
    override fun getString(columnLabel: String): String? {
        return getString(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnLabel: String): Boolean {
        return getBoolean(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getByte(columnLabel: String): Byte {
        return getByte(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getShort(columnLabel: String): Short {
        return getShort(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getInt(columnLabel: String): Int {
        return getInt(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getLong(columnLabel: String): Long {
        return getLong(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getFloat(columnLabel: String): Float {
        return getFloat(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getDouble(columnLabel: String): Double {
        return getDouble(checkColumn(columnLabel))
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal? {
        return getBigDecimal(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getBytes(columnLabel: String): ByteArray? {
        return getBytes(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getDate(columnLabel: String): Date? {
        return getDate(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getTime(columnLabel: String): Time? {
        return getTime(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnLabel: String): Timestamp? {
        return getTimestamp(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnLabel: String): InputStream? {
        return getAsciiStream(checkColumn(columnLabel))
    }

    @Deprecated("")
    @Throws(SQLException::class)
    override fun getUnicodeStream(columnLabel: String): InputStream? {
        return getUnicodeStream(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnLabel: String): InputStream? {
        return getBinaryStream(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning? {
        return null
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {
    }

    @Throws(SQLException::class)
    override fun getCursorName(): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun getMetaData(): ResultSetMetaData {
        return metaData
    }

    @Throws(SQLException::class)
    override fun getObject(columnIndex: Int): Any? {
        return checkColumn(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getObject(columnLabel: String): Any? {
        return getObject(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun findColumn(columnLabel: String): Int {
        val colIdx = columnNames!![columnLabel]
        return colIdx ?: 0
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnIndex: Int): Reader? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnLabel: String): Reader? {
        return getCharacterStream(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        val `val` = checkColumn(columnIndex) ?: return null
        if (`val` is BigDecimal) return `val`
        return if (`val` is Number) BigDecimal(`val`.toDouble()) else BigDecimal(`val`.toString().toDouble())
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnLabel: String): BigDecimal? {
        return getBigDecimal(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun isBeforeFirst(): Boolean {
        return currentPos == 0
    }

    @Throws(SQLException::class)
    override fun isAfterLast(): Boolean {
        return currentPos == 0 && nextPos == 0
    }

    @Throws(SQLException::class)
    override fun isFirst(): Boolean {
        return currentPos == 1
    }

    @Throws(SQLException::class)
    override fun isLast(): Boolean {
        return nextPos == 0 && currentPos != 0
    }

    @Throws(SQLException::class)
    override fun beforeFirst() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun afterLast() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun first(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun last(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getRow(): Int {
        return currentPos
    }

    @Throws(SQLException::class)
    override fun absolute(row: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun relative(rows: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun previous(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun setFetchDirection(direction: Int) {
    }

    @Throws(SQLException::class)
    override fun getFetchDirection(): Int {
        return statement.fetchDirection
    }

    @Throws(SQLException::class)
    override fun setFetchSize(rows: Int) {
    }

    @Throws(SQLException::class)
    override fun getFetchSize(): Int {
        return statement.fetchSize
    }

    @Throws(SQLException::class)
    override fun getType(): Int {
        return statement.resultSetType
    }

    @Throws(SQLException::class)
    override fun getConcurrency(): Int {
        return statement.resultSetConcurrency
    }

    @Throws(SQLException::class)
    override fun rowUpdated(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun rowInserted(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun rowDeleted(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun updateNull(columnIndex: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateByte(columnIndex: Int, x: Byte) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateShort(columnIndex: Int, x: Short) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateInt(columnIndex: Int, x: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateLong(columnIndex: Int, x: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateFloat(columnIndex: Int, x: Float) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDouble(columnIndex: Int, x: Double) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateString(columnIndex: Int, x: String?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDate(columnIndex: Int, x: Date?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTime(columnIndex: Int, x: Time?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(columnIndex: Int, x: Any?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNull(columnLabel: String) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBoolean(columnLabel: String, x: Boolean) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateByte(columnLabel: String, x: Byte) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateShort(columnLabel: String, x: Short) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateInt(columnLabel: String, x: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateLong(columnLabel: String, x: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateFloat(columnLabel: String, x: Float) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDouble(columnLabel: String, x: Double) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateString(columnLabel: String, x: String?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBytes(columnLabel: String, x: ByteArray?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDate(columnLabel: String, x: Date?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTime(columnLabel: String, x: Time?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTimestamp(columnLabel: String, x: Timestamp?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(columnLabel: String, x: Any?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun insertRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun deleteRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun refreshRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun cancelRowUpdates() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun moveToInsertRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun moveToCurrentRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getStatement(): Statement {
        return statement
    }

    @Throws(SQLException::class)
    override fun getObject(columnIndex: Int, map: Map<String?, Class<*>?>?): Any {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRef(columnIndex: Int): Ref? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getBlob(columnIndex: Int): Blob? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getClob(columnIndex: Int): Clob? {
        val `val` = checkColumn(columnIndex) ?: return null
        return ClobString(`val`.toString())
    }

    @Throws(SQLException::class)
    override fun getArray(columnIndex: Int): java.sql.Array? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getObject(columnLabel: String, map: Map<String?, Class<*>?>?): Any? {
        return getObject(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getRef(columnLabel: String): Ref? {
        return getRef(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getBlob(columnLabel: String): Blob? {
        return getBlob(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getClob(columnLabel: String): Clob? {
        return getClob(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getArray(columnLabel: String): java.sql.Array? {
        return getArray(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getDate(columnIndex: Int, cal: Calendar): Date? {
        return getDate(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getDate(columnLabel: String, cal: Calendar): Date? {
        return getDate(columnLabel)
    }

    @Throws(SQLException::class)
    override fun getTime(columnIndex: Int, cal: Calendar): Time? {
        return getTime(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getTime(columnLabel: String, cal: Calendar): Time? {
        return getTime(columnLabel)
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnIndex: Int, cal: Calendar): Timestamp? {
        return getTimestamp(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnLabel: String, cal: Calendar): Timestamp? {
        return getTimestamp(columnLabel)
    }

    @Throws(SQLException::class)
    override fun getURL(columnIndex: Int): URL? {
        val `val` = checkColumn(columnIndex) ?: return null
        return if (`val` is URL) `val` else try {
            URL(`val`.toString())
        } catch (e: MalformedURLException) {
            throw SQLException("Cannot extract url: $`val`", e)
        }
    }

    @Throws(SQLException::class)
    override fun getURL(columnLabel: String): URL? {
        return getURL(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun updateRef(columnIndex: Int, x: Ref?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRef(columnLabel: String, x: Ref?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, x: Blob?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, x: Blob?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, x: Clob?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, x: Clob?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateArray(columnIndex: Int, x: java.sql.Array?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateArray(columnLabel: String, x: java.sql.Array?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRowId(columnIndex: Int): RowId? {
        val `val` = checkColumn(columnIndex) ?: return null
        if (`val` is RowId) return `val`
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRowId(columnLabel: String): RowId? {
        return getRowId(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun updateRowId(columnIndex: Int, x: RowId?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRowId(columnLabel: String, x: RowId?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        return statement.resultSetHoldability
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return closed
    }

    @Throws(SQLException::class)
    override fun updateNString(columnIndex: Int, nString: String) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNString(columnLabel: String, nString: String) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, nClob: NClob) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, nClob: NClob) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNClob(columnIndex: Int): NClob? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getNClob(columnLabel: String): NClob? {
        return getNClob(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnIndex: Int): SQLXML? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnLabel: String): SQLXML? {
        return getSQLXML(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateSQLXML(columnLabel: String, xmlObject: SQLXML) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNString(columnIndex: Int): String? {
        return getString(columnIndex)
    }

    @Throws(SQLException::class)
    override fun getNString(columnLabel: String): String? {
        return getString(columnLabel)
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnIndex: Int): Reader? {
        checkColumn(columnIndex)
        return null
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnLabel: String): Reader? {
        return getNCharacterStream(checkColumn(columnLabel))
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnIndex: Int, x: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnLabel: String, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, inputStream: InputStream, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, inputStream: InputStream, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, reader: Reader, length: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnIndex: Int, x: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(columnLabel: String, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(columnLabel: String, x: InputStream?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(columnLabel: String, x: InputStream?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(columnLabel: String, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnIndex: Int, inputStream: InputStream) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(columnLabel: String, inputStream: InputStream) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnIndex: Int, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(columnLabel: String, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnIndex: Int, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(columnLabel: String, reader: Reader) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun <T> getObject(columnIndex: Int, type: Class<T>): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun <T> getObject(columnLabel: String, type: Class<T>): T {
        return getObject(checkColumn(columnLabel), type)
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException()
    }
}