/**
 * Copyright 2016 Emmanuel Keller / QWAZR
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
package com.qwazr.jdbc.cache;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.SortedMap;
import java.util.TreeMap;

class CachedPreparedStatement<T extends PreparedStatement> extends CachedStatement<T> implements PreparedStatement {

    protected final SortedMap<Integer, Object> parameters;

    CachedPreparedStatement(final CachedConnection connection, final ResultSetCacheImpl resultSetCache,
            final T backendStatement, final String sql, final int resultSetConcurrency, final int resultSetType,
            final int resultSetHoldability) {
        super(connection, resultSetCache, backendStatement, resultSetConcurrency, resultSetType, resultSetHoldability);
        this.parameters = new TreeMap<>();
        this.executedSql = sql;
    }

    CachedPreparedStatement(final CachedConnection connection, final ResultSetCacheImpl resultSetCache,
            final T backendStatement, final String sql) {
        this(connection, resultSetCache, backendStatement, sql, 0, 0, 0);
    }

    @Override
    protected void generateKey() throws SQLException {
        final StringBuilder sb = new StringBuilder(executedSql);
        parameters.forEach((index, value) -> {
            sb.append('•');
            sb.append(index);
            sb.append(value.toString());
        });
        generatedKey = generateCacheKey(sb.toString());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        generateKey();
        return resultSetCache.get(this, generatedKey, () -> backendStatement.executeQuery());
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeUpdate();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNull(parameterIndex, sqlType);
        parameters.remove(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBoolean(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setByte(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setShort(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setInt(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setLong(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setFloat(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDouble(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBigDecimal(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setString(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBytes(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDate(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTime(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTimestamp(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setUnicodeStream(parameterIndex, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        if (backendStatement != null)
            backendStatement.clearParameters();
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterIndex, x, targetSqlType);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        generateKey();
        if (resultSetCache.checkIfExists(generatedKey))
            return true;
        if (backendStatement != null)
            return backendStatement.execute();
        else
            throw new SQLException("No cache entry");
    }

    @Override
    public void addBatch() throws SQLException {
        if (backendStatement != null)
            backendStatement.addBatch();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterIndex, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setRef(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setArray(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getMetaData();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDate(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTime(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTimestamp(parameterIndex, x, cal);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNull(parameterIndex, sqlType, typeName);
        parameters.remove(parameterIndex);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setURL(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getParameterMetaData();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setRowId(parameterIndex, x);
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNString(parameterIndex, value);
        parameters.put(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNCharacterStream(parameterIndex, value, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterIndex, value);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterIndex, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterIndex, inputStream, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterIndex, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (backendStatement != null)
            backendStatement.setSQLXML(parameterIndex, xmlObject);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterIndex, targetSqlType, scaleOrLength);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterIndex, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterIndex, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterIndex, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterIndex, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterIndex, reader);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNCharacterStream(parameterIndex, value);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterIndex, reader);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterIndex, inputStream);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterIndex, reader);
        else
            throw new SQLFeatureNotSupportedException();
    }
}
