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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

abstract class ResultSetCacheImpl implements ResultSetCache {

    private final ConcurrentHashMap<String, ReentrantLock> activeKeys;
    private final ConcurrentHashMap<String, ByteArrayOutputStream> cache;

    ResultSetCacheImpl() {
        this.activeKeys = new ConcurrentHashMap<>();
        this.cache = new ConcurrentHashMap<>();
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

    private ConcurrentHashMap<String, ReentrantLock> checkCacheMap() {
        return Objects.requireNonNull(activeKeys, "No cache");
    }

    private CachedStatement checkCachedStatement(final Statement stmt) throws SQLException {
        Objects.requireNonNull(stmt, "The statement is null");
        if (stmt instanceof CachedStatement)
            return (CachedStatement) stmt;
        throw new SQLException("The statement is not cached");
    }

    String checkKey(final Statement stmt) throws SQLException {
        return Objects.requireNonNull(checkCachedStatement(stmt).getOrGenerateKey(), "No key found");
    }

    @Override
    public int active() {
        return checkCacheMap().size();
    }

    @Override
    public boolean active(Statement stmt) throws SQLException {
        return checkCacheMap().containsKey(checkKey(stmt));
    }
}
