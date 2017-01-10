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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

class CachedCallableStatement extends CachedPreparedStatement<CallableStatement> implements CallableStatement {

    protected final SortedMap<String, Object> namedParameters;

    CachedCallableStatement(final CachedConnection connection, final ResultSetCacheImpl resultSetCache,
            final CallableStatement backendStatement, final String sql, final int resultSetConcurrency,
            final int resultSetType, final int resultSetHoldability) {
        super(connection, resultSetCache, backendStatement, sql, resultSetConcurrency, resultSetType,
                resultSetHoldability);
        this.namedParameters = new TreeMap<>();
    }

    CachedCallableStatement(final CachedConnection connection, final ResultSetCacheImpl resultSetCache,
            final CallableStatement backendStatement, final String sql) {
        this(connection, resultSetCache, backendStatement, sql, 0, 0, 0);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterIndex, sqlType);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterIndex, sqlType, scale);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (backendStatement != null)
            return backendStatement.wasNull();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getString(parameterIndex);
        else
            return (String) parameters.get(parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBoolean(parameterIndex);
        else
            return (Boolean) parameters.get(parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getByte(parameterIndex);
        else
            return (Byte) parameters.get(parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getShort(parameterIndex);
        else
            return (Short) parameters.get(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getInt(parameterIndex);
        else
            return (Integer) parameters.get(parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getLong(parameterIndex);
        else
            return (Long) parameters.get(parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getFloat(parameterIndex);
        else
            return (Float) parameters.get(parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDouble(parameterIndex);
        else
            return (Double) parameters.get(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBigDecimal(parameterIndex);
        else
            return (BigDecimal) parameters.get(parameterIndex);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBytes(parameterIndex);
        else
            return (byte[]) parameters.get(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDate(parameterIndex);
        else
            return (Date) parameters.get(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTime(parameterIndex);
        else
            return (Time) parameters.get(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTimestamp(parameterIndex);
        else
            return (Timestamp) parameters.get(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterIndex);
        else
            return parameters.get(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBigDecimal(parameterIndex);
        else
            return (BigDecimal) parameters.get(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterIndex, map);
        else
            return parameters.get(parameterIndex);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getRef(parameterIndex);
        else
            return (Ref) parameters.get(parameterIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBlob(parameterIndex);
        else
            return (Blob) parameters.get(parameterIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getClob(parameterIndex);
        else
            return (Clob) parameters.get(parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getArray(parameterIndex);
        else
            return (Array) parameters.get(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDate(parameterIndex);
        else
            return (Date) parameters.get(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTime(parameterIndex, cal);
        else
            return (Time) parameters.get(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTimestamp(parameterIndex, cal);
        else
            return (Timestamp) parameters.get(parameterIndex);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterName, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterName, sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        if (backendStatement != null)
            backendStatement.registerOutParameter(parameterName, sqlType, typeName);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getURL(parameterIndex);
        else
            return (URL) parameters.get(parameterIndex);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        if (backendStatement != null)
            backendStatement.setURL(parameterName, val);
        namedParameters.put(parameterName, val);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNull(parameterName, sqlType);
        namedParameters.remove(parameterName);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBoolean(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setByte(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setShort(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setInt(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setLong(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setFloat(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDouble(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBigDecimal(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setString(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBytes(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDate(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTime(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTimestamp(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterName, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterName, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterName, x, targetSqlType, scale);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterName, x, targetSqlType);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setObject(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterName, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setDate(parameterName, x, cal);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTime(parameterName, x, cal);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        if (backendStatement != null)
            backendStatement.setTimestamp(parameterName, x, cal);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNull(parameterName, sqlType, typeName);
        namedParameters.remove(parameterName);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getString(parameterName);
        else
            return (String) namedParameters.get(parameterName);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBoolean(parameterName);
        else
            return (Boolean) namedParameters.get(parameterName);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getByte(parameterName);
        else
            return (Byte) namedParameters.get(parameterName);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getShort(parameterName);
        else
            return (Short) namedParameters.get(parameterName);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getInt(parameterName);
        else
            return (Integer) namedParameters.get(parameterName);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getLong(parameterName);
        else
            return (Long) namedParameters.get(parameterName);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getFloat(parameterName);
        else
            return (Float) namedParameters.get(parameterName);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDouble(parameterName);
        else
            return (Double) namedParameters.get(parameterName);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBytes(parameterName);
        else
            return (byte[]) namedParameters.get(parameterName);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDate(parameterName);
        else
            return (Date) namedParameters.get(parameterName);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTime(parameterName);
        else
            return (Time) namedParameters.get(parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTimestamp(parameterName);
        else
            return (Timestamp) namedParameters.get(parameterName);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterName);
        else
            return namedParameters.get(parameterName);
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBigDecimal(parameterName);
        else
            return (BigDecimal) namedParameters.get(parameterName);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterName, map);
        else
            return namedParameters.get(parameterName);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getRef(parameterName);
        else
            return (Ref) namedParameters.get(parameterName);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getBlob(parameterName);
        else
            return (Blob) namedParameters.get(parameterName);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getClob(parameterName);
        else
            return (Clob) namedParameters.get(parameterName);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getArray(parameterName);
        else
            return (Array) namedParameters.get(parameterName);
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getDate(parameterName);
        else
            return (Date) namedParameters.get(parameterName);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTime(parameterName);
        else
            return (Time) namedParameters.get(parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getTimestamp(parameterName, cal);
        else
            return (Timestamp) namedParameters.get(parameterName);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getURL(parameterName);
        else
            return (URL) namedParameters.get(parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getRowId(parameterIndex);
        else
            return (RowId) parameters.get(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getRowId(parameterName);
        else
            return (RowId) namedParameters.get(parameterName);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setRowId(parameterName, x);
        namedParameters.put(parameterName, x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNString(parameterName, value);
        namedParameters.put(parameterName, value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNCharacterStream(parameterName, value, length);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterName, value);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterName, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterName, inputStream, length);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterName, reader, length);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNClob(parameterIndex);
        else
            return (NClob) parameters.get(parameterIndex);
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNClob(parameterName);
        else
            return (NClob) namedParameters.get(parameterName);
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        if (backendStatement != null)
            backendStatement.setSQLXML(parameterName, xmlObject);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getSQLXML(parameterIndex);
        else
            return (SQLXML) parameters.get(parameterIndex);
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getSQLXML(parameterName);
        else
            return (SQLXML) namedParameters.get(parameterName);
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNString(parameterIndex);
        else
            return (String) parameters.get(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNString(parameterName);
        else
            return (String) namedParameters.get(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNCharacterStream(parameterIndex);
        else
            return (Reader) parameters.get(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getNCharacterStream(parameterName);
        else
            return (Reader) namedParameters.get(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getCharacterStream(parameterIndex);
        else
            return (Reader) parameters.get(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getCharacterStream(parameterName);
        else
            return (Reader) namedParameters.get(parameterName);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterName, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterName, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterName, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterName, x, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterName, reader, length);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setAsciiStream(parameterName, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBinaryStream(parameterName, x);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCharacterStream(parameterName, reader);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNCharacterStream(parameterName, value);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setClob(parameterName, reader);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        if (backendStatement != null)
            backendStatement.setBlob(parameterName, inputStream);
        else
            throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        if (backendStatement != null)
            backendStatement.setNClob(parameterName, reader);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterIndex, type);
        else
            return (T) parameters.get(parameterIndex);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getObject(parameterName, type);
        else
            return (T) namedParameters.get(parameterName);
    }
}
