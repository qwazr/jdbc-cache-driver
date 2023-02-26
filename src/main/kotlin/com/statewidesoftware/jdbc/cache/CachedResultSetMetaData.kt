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

import com.statewidesoftware.jdbc.cache.ResultSetWriter.ColumnDef
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException

internal class CachedResultSetMetaData(@JvmField val columns: Array<ColumnDef>) : ResultSetMetaData {
    @Throws(SQLException::class)
    override fun getColumnCount(): Int {
        return columns.size
    }

    @Throws(SQLException::class)
    private fun getColumns(column: Int): ColumnDef {
        if (column == 0 || column > columns.size) throw SQLException("Wrong column number: $column")
        return columns[column - 1]
    }

    @Throws(SQLException::class)
    override fun isAutoIncrement(column: Int): Boolean {
        return getColumns(column).isAutoIncrement
    }

    @Throws(SQLException::class)
    override fun isCaseSensitive(column: Int): Boolean {
        return getColumns(column).isCaseSensitive
    }

    @Throws(SQLException::class)
    override fun isSearchable(column: Int): Boolean {
        return getColumns(column).isSearchable
    }

    @Throws(SQLException::class)
    override fun isCurrency(column: Int): Boolean {
        return getColumns(column).isCurrency
    }

    @Throws(SQLException::class)
    override fun isNullable(column: Int): Int {
        return getColumns(column).isNullable
    }

    @Throws(SQLException::class)
    override fun isSigned(column: Int): Boolean {
        return getColumns(column).isSigned
    }

    @Throws(SQLException::class)
    override fun getColumnDisplaySize(column: Int): Int {
        return getColumns(column).displaySize
    }

    @Throws(SQLException::class)
    override fun getColumnLabel(column: Int): String {
        return getColumns(column).label
    }

    @Throws(SQLException::class)
    override fun getColumnName(column: Int): String {
        return getColumns(column).name
    }

    @Throws(SQLException::class)
    override fun getSchemaName(column: Int): String {
        return getColumns(column).schemaName
    }

    @Throws(SQLException::class)
    override fun getPrecision(column: Int): Int {
        return getColumns(column).precision
    }

    @Throws(SQLException::class)
    override fun getScale(column: Int): Int {
        return getColumns(column).scale
    }

    @Throws(SQLException::class)
    override fun getTableName(column: Int): String {
        return getColumns(column).tableName
    }

    @Throws(SQLException::class)
    override fun getCatalogName(column: Int): String {
        return getColumns(column).catalog
    }

    @Throws(SQLException::class)
    override fun getColumnType(column: Int): Int {
        return getColumns(column).type
    }

    @Throws(SQLException::class)
    override fun getColumnTypeName(column: Int): String {
        return getColumns(column).typeName
    }

    @Throws(SQLException::class)
    override fun isReadOnly(column: Int): Boolean {
        return getColumns(column).isReadOnly
    }

    @Throws(SQLException::class)
    override fun isWritable(column: Int): Boolean {
        return getColumns(column).isWritable
    }

    @Throws(SQLException::class)
    override fun isDefinitelyWritable(column: Int): Boolean {
        return getColumns(column).isDefinitelyWritable
    }

    @Throws(SQLException::class)
    override fun getColumnClassName(column: Int): String {
        return getColumns(column).className
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