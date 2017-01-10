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

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

class CachedConnection implements Connection {

    private volatile boolean autocommit;
    private volatile boolean closed;
    private volatile boolean readOnly;
    private volatile String catalog;
    private volatile int transactionIsolation;
    private volatile Map<String, Class<?>> typeMap;
    private volatile int holdability;
    private final Properties clientInfos;
    private volatile String schema;

    private final Connection connection;
    private final ResultSetCacheImpl resultSetCache;

    CachedConnection(final Connection backendConnection, final ResultSetCacheImpl resultSetCache) throws SQLException {
        this.connection = backendConnection;
        this.resultSetCache = resultSetCache;
        this.autocommit = false;
        this.closed = false;
        this.readOnly = false;
        this.catalog = null;
        this.transactionIsolation = TRANSACTION_NONE;
        this.typeMap = null;
        this.holdability = 0;
        this.clientInfos = new Properties();
        this.schema = null;
    }

    ResultSetCacheImpl getResultSetCache() {
        return resultSetCache;
    }

    @Override
    public Statement createStatement() throws SQLException {
        final Statement statement = connection == null ? null : connection.createStatement();
        return resultSetCache != null ? new CachedStatement<>(this, resultSetCache, statement) : statement;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        final PreparedStatement statement = connection == null ? null : connection.prepareStatement(sql);
        return resultSetCache != null ? new CachedPreparedStatement<>(this, resultSetCache, statement, sql) : statement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        final CallableStatement statement = connection == null ? null : connection.prepareCall(sql);
        return resultSetCache != null ? new CachedCallableStatement(this, resultSetCache, statement, sql) : statement;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (connection != null)
            return connection.nativeSQL(sql);
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (connection != null)
            connection.setAutoCommit(autoCommit);
        this.autocommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return connection == null ? autocommit : connection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        if (connection != null)
            connection.commit();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        if (connection != null)
            connection.commit();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        if (connection != null)
            connection.close();
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection != null ? connection.isClosed() : closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (connection != null)
            return connection.getMetaData();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        if (connection != null)
            connection.setReadOnly(readOnly);
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection != null ? connection.isReadOnly() : readOnly;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        if (connection != null)
            connection.setCatalog(catalog);
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return connection != null ? connection.getCatalog() : catalog;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        if (connection != null)
            connection.setTransactionIsolation(level);
        transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection != null ? connection.getWarnings() : null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (connection != null)
            connection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        final Statement statement =
                connection == null ? null : connection.createStatement(resultSetType, resultSetConcurrency);
        return resultSetCache != null ?
                new CachedStatement<>(this, resultSetCache, statement, resultSetType, resultSetConcurrency, 0) :
                statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        final PreparedStatement statement =
                connection == null ? null : connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        return resultSetCache != null ?
                new CachedPreparedStatement<>(this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
                        0) :
                statement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        final CallableStatement statement =
                connection == null ? null : connection.prepareCall(sql, resultSetType, resultSetConcurrency);
        return resultSetCache != null ?
                new CachedCallableStatement(this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
                        0) :
                statement;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connection != null ? connection.getTypeMap() : typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        if (connection != null)
            connection.setTypeMap(map);
        this.typeMap = map;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (connection != null)
            connection.setHoldability(holdability);
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        return connection != null ? connection.getHoldability() : holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        if (connection != null)
            return connection.setSavepoint();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        if (connection != null)
            return connection.setSavepoint(name);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (connection != null)
            connection.rollback(savepoint);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (connection != null)
            connection.releaseSavepoint(savepoint);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        final Statement statement = connection == null ?
                null :
                connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        return resultSetCache != null ?
                new CachedStatement<>(this, resultSetCache, statement, resultSetType, resultSetConcurrency,
                        resultSetHoldability) :
                statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        final PreparedStatement statement = connection == null ?
                null :
                connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return resultSetCache != null ?
                new CachedPreparedStatement<>(this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
                        resultSetHoldability) :
                statement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        final CallableStatement statement = connection == null ?
                null :
                connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return resultSetCache != null ?
                new CachedCallableStatement(this, resultSetCache, statement, sql, resultSetType, resultSetConcurrency,
                        resultSetHoldability) :
                statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        final PreparedStatement statement =
                connection == null ? null : connection.prepareStatement(sql, autoGeneratedKeys);
        return resultSetCache != null ? new CachedPreparedStatement<>(this, resultSetCache, statement, sql) : statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        final PreparedStatement statement = connection == null ? null : connection.prepareStatement(sql, columnIndexes);
        return resultSetCache != null ? new CachedPreparedStatement<>(this, resultSetCache, statement, sql) : statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        final PreparedStatement statement = connection == null ? null : connection.prepareStatement(sql, columnNames);
        return resultSetCache != null ? new CachedPreparedStatement<>(this, resultSetCache, statement, sql) : statement;
    }

    @Override
    public Clob createClob() throws SQLException {
        if (connection != null)
            return connection.createClob();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        if (connection != null)
            return connection.createBlob();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        if (connection != null)
            return connection.createNClob();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        if (connection != null)
            return connection.createSQLXML();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (connection != null)
            return connection.isValid(timeout);
        else
            return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        synchronized (clientInfos) {
            this.clientInfos.put(name, value);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        synchronized (clientInfos) {
            clientInfos.clear();
            clientInfos.putAll(properties);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        synchronized (clientInfos) {
            return clientInfos.getProperty(name);
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        synchronized (clientInfos) {
            return new Properties(clientInfos);
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        if (connection != null)
            return connection.createArrayOf(typeName, elements);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        if (connection != null)
            return connection.createStruct(typeName, attributes);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (connection != null)
            connection.setSchema(schema);
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (connection != null)
            connection.abort(executor);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        if (connection != null)
            connection.setNetworkTimeout(executor, milliseconds);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        if (connection != null)
            return connection.getNetworkTimeout();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (connection != null)
            return connection.unwrap(iface);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (connection != null)
            return connection.isWrapperFor(iface);
        else
            throw new SQLFeatureNotSupportedException();
    }

}
