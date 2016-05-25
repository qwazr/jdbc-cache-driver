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

class CachedStatement<T extends Statement> implements Statement {

    protected final CachedConnection connection;
    protected final T backendStatement;

    private final int resultSetConcurrency;
    private final int resultSetType;
    private final int resultSetHoldability;

    private volatile int maxFieldSize;
    private volatile int maxRows;
    private volatile int queryTimeOut;
    private volatile CachedResultSet resultSet;
    private volatile int fetchDirection;
    private volatile int fetchSize;
    private volatile boolean closed;
    private volatile boolean poolable;
    private volatile boolean closeOnCompletion;

    CachedStatement(final CachedConnection connection, final T backendStatement, final int resultSetConcurrency,
            final int resultSetType, final int resultSetHoldability) {
        this.connection = connection;
        this.backendStatement = backendStatement;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetType = resultSetType;
        this.resultSetHoldability = resultSetHoldability;
        this.maxFieldSize = 0;
        this.maxRows = 0;
        this.queryTimeOut = 0;
        this.resultSet = null;
        this.fetchDirection = 0;
        this.fetchSize = 0;
        this.closed = false;
        this.poolable = false;
        this.closeOnCompletion = false;
    }

    CachedStatement(final CachedConnection connection, final T backendStatement) {
        this(connection, backendStatement, 0, 0, 0);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        final String cacheKey = ResultSetCache.getKey(sql);
        return connection.resultSetCache.get(this, cacheKey, () -> backendStatement.executeQuery(sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeUpdate(sql);
        else
            throw new SQLFeatureNotSupportedException();
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
        if (backendStatement != null)
            return backendStatement.execute(sql);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getUpdateCount();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getMoreResults();
        else
            throw new SQLFeatureNotSupportedException();
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
        if (backendStatement != null)
            backendStatement.addBatch(sql);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        if (backendStatement != null)
            backendStatement.clearBatch();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeBatch();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (backendStatement != null)
            return backendStatement.getMoreResults(current);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (backendStatement != null)
            return backendStatement.getGeneratedKeys();
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeUpdate(sql, autoGeneratedKeys);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeUpdate(sql, columnIndexes);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (backendStatement != null)
            return backendStatement.executeUpdate(sql, columnNames);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (backendStatement != null)
            return backendStatement.execute(sql, autoGeneratedKeys);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (backendStatement != null)
            return backendStatement.execute(sql, columnIndexes);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (backendStatement != null)
            return backendStatement.execute(sql, columnNames);
        else
            throw new SQLFeatureNotSupportedException();
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
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (backendStatement != null)
            return backendStatement.unwrap(iface);
        else
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (backendStatement != null)
            return backendStatement.isWrapperFor(iface);
        else
            throw new SQLFeatureNotSupportedException();
    }
}
