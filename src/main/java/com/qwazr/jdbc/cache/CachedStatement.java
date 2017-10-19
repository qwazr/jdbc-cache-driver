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
package com.qwazr.jdbc.cache;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

class CachedStatement<T extends Statement> implements Statement {

    private final CachedConnection connection;
    final ResultSetCache resultSetCache;
    final T backendStatement;

    private final int resultSetConcurrency;
    private final int resultSetType;
    private final int resultSetHoldability;

    private volatile int maxFieldSize;
    private volatile int maxRows;
    private volatile int queryTimeOut;
    private volatile int fetchDirection;
    private volatile int fetchSize;
    private volatile boolean closed;
    private volatile boolean poolable;
    private volatile boolean closeOnCompletion;

    volatile String executedSql;
    volatile String generatedKey;

    CachedStatement(final CachedConnection connection, final ResultSetCache resultSetCache,
            final T backendStatement, final int resultSetConcurrency, final int resultSetType,
            final int resultSetHoldability) {
        this.connection = connection;
        this.resultSetCache = resultSetCache;
        this.backendStatement = backendStatement;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetType = resultSetType;
        this.resultSetHoldability = resultSetHoldability;
        this.maxFieldSize = 0;
        this.maxRows = 0;
        this.queryTimeOut = 0;
        this.fetchDirection = 0;
        this.fetchSize = 0;
        this.closed = false;
        this.poolable = false;
        this.closeOnCompletion = false;
        this.executedSql = null;
    }

    CachedStatement(final CachedConnection connection, final ResultSetCache resultSetCache,
            final T backendStatement) {
        this(connection, resultSetCache, backendStatement, 0, 0, 0);
    }

    final T checkBackendStatement() throws SQLException {
        return checkBackendStatement(null);
    }

    final T checkBackendStatement(final String error) throws SQLException {
        if (backendStatement != null)
            return backendStatement;
        else
            throw error == null ? new SQLFeatureNotSupportedException() : new SQLException(error);
    }

    static String generateCacheKey(final String src) throws SQLException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return DatatypeConverter.printHexBinary(md.digest(src.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException("MD5 is not available");
        }
    }

    protected void generateKey() throws SQLException {
        generatedKey = generateCacheKey(executedSql);
    }

    final String getOrGenerateKey() throws SQLException {
        if (generatedKey == null)
            generateKey();
        return generatedKey;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        this.executedSql = sql;
        generateKey();
        return resultSetCache
                .get(this, generatedKey, backendStatement == null ? null : () -> backendStatement.executeQuery(sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        this.executedSql = sql;
        return checkBackendStatement().executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        if (backendStatement != null)
            backendStatement.close();
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getMaxFieldSize();
        else
            return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (backendStatement != null)
            backendStatement.setMaxFieldSize(max);
        this.maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getMaxRows();
        else
            return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (backendStatement != null)
            backendStatement.setMaxRows(max);
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (backendStatement != null)
            backendStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getQueryTimeout();
        else
            return queryTimeOut;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (backendStatement != null)
            backendStatement.setQueryTimeout(seconds);
        this.queryTimeOut = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        if (backendStatement != null)
            backendStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getWarnings();
        else
            return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (backendStatement != null)
            backendStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        if (backendStatement != null)
            backendStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        this.executedSql = sql;
        generateKey();
        return resultSetCache.checkIfExists(generatedKey) || checkBackendStatement("No cache entry").execute(sql);
    }

    @Override
    final public ResultSet getResultSet() throws SQLException {
        generateKey();
        return resultSetCache.get(this, generatedKey, backendStatement == null ? null : backendStatement::getResultSet);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return checkBackendStatement().getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return checkBackendStatement().getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (backendStatement != null)
            backendStatement.setFetchDirection(direction);
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getFetchDirection();
        else
            return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (backendStatement != null)
            backendStatement.setFetchSize(rows);
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getFetchSize();
        else
            return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkBackendStatement().addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkBackendStatement().clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return checkBackendStatement().executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return checkBackendStatement().getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return checkBackendStatement().getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        executedSql = sql;
        return checkBackendStatement().executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        executedSql = sql;
        return checkBackendStatement().executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        executedSql = sql;
        return checkBackendStatement().executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        executedSql = sql;
        generateKey();
        return resultSetCache.checkIfExists(generatedKey) || checkBackendStatement("No cache entry")
                .execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        executedSql = sql;
        generateKey();
        return resultSetCache.checkIfExists(generatedKey) || checkBackendStatement("No cache entry")
                .execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        executedSql = sql;
        generateKey();
        return resultSetCache.checkIfExists(generatedKey) || checkBackendStatement("No cache entry")
                .execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        if (backendStatement != null)
            return backendStatement.isClosed();
        else
            return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (backendStatement != null)
            backendStatement.setPoolable(poolable);
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (backendStatement != null)
            return backendStatement.isPoolable();
        else
            return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (backendStatement != null)
            backendStatement.closeOnCompletion();
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (backendStatement != null)
            return backendStatement.isCloseOnCompletion();
        else
            return closeOnCompletion;
    }

    @Override
    public <V> V unwrap(Class<V> iface) throws SQLException {
        return checkBackendStatement().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return checkBackendStatement().isWrapperFor(iface);
    }
}
