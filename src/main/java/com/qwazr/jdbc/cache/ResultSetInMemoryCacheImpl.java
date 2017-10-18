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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ResultSetInMemoryCacheImpl extends ResultSetCacheImpl {

    private final ConcurrentHashMap<String, ReentrantLock> activeKeys;
    private final ConcurrentHashMap<String, ByteArrayOutputStream> cache;

    ResultSetInMemoryCacheImpl() {
        this.activeKeys = new ConcurrentHashMap<>();
        this.cache = new ConcurrentHashMap<>();
    }

    /**
     * Return the cached ResultSet for the given key.
     * If the entry does not exist the ResultSet is extracted by calling the given resultSetProvider.
     * If the entry does not exist and no resultSetProvider is given (null) an SQLException is thrown.
     *
     * @param statement         the cached statement
     * @param key               the generated key for this statement
     * @param resultSetProvider the optional result provider
     * @return the cached ResultSet
     * @throws SQLException if the statement cannot be executed
     */
    public ResultSet get(final CachedStatement statement, final String key, final ResultSetCache.Provider resultSetProvider)
            throws SQLException {
        if (!cache.containsKey(key)) {
            if (resultSetProvider == null)
                throw new SQLException("No cache available");
            try {
                buildCache(key, resultSetProvider);
            } catch (IOException e) {
                throw new SQLException("Can not read cache", e);
            }
        }
        return new CachedInMemoryResultSet(statement, cache.get(key));
    }

    private void buildCache(final String key, final Provider resultSetProvider)
            throws SQLException, IOException {
        final Lock keyLock = activeKeys.computeIfAbsent(key, s -> new ReentrantLock(true));
        try {
            keyLock.lock();
            try {
                final ResultSet providedResultSet = resultSetProvider.provide();
                ByteArrayOutputStream outputStream = ResultSetWriter.write(providedResultSet);
                cache.put(key, outputStream);
            } finally {
                keyLock.unlock();
            }
        } finally {
            activeKeys.remove(key);
        }
    }

    /**
     * Check if an entry is available for this key.
     *
     * @param key the computed key
     * @return always true if the cache entry exists
     */

    public boolean checkIfExists(final String key) {
        return cache.containsKey(key);
    }

    @Override
    public void flush() throws SQLException {
        cache.clear();
    }

    @Override
    public void flush(final Statement stmt) throws SQLException {
        cache.remove(checkKey(stmt));
    }

    @Override
    public int size() throws SQLException {
        return cache.size();
    }

    @Override
    public boolean exists(Statement stmt) throws SQLException {
        return cache.containsKey(checkKey(stmt));
    }
}
