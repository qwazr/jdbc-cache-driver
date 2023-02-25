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
package com.statewidesoftware.jdbc.cache;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

class CachedResultSetMetaData implements ResultSetMetaData {

    final ResultSetWriter.ColumnDef[] columns;

    CachedResultSetMetaData(ResultSetWriter.ColumnDef[] columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    private ResultSetWriter.ColumnDef getColumns(final int column) throws SQLException {
        if (column == 0 || column > columns.length)
            throw new SQLException("Wrong column number: " + column);
        return columns[column - 1];
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumns(column).isAutoIncrement;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumns(column).isCaseSensitive;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return getColumns(column).isSearchable;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return getColumns(column).isCurrency;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumns(column).isNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumns(column).isSigned;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumns(column).displaySize;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumns(column).label;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumns(column).name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getColumns(column).schemaName;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumns(column).precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumns(column).scale;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumns(column).tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumns(column).catalog;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumns(column).type;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumns(column).typeName;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getColumns(column).isReadOnly;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return getColumns(column).isWritable;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumns(column).isDefinitelyWritable;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumns(column).className;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
