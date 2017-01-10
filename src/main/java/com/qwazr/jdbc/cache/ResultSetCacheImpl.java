/**
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ResultSetCacheImpl implements ResultSetCache {

    private final Path cacheDirectory;
    private final ConcurrentHashMap<String, ReentrantLock> activeKeys;

    ResultSetCacheImpl(final Path cacheDirectory) {
        if (!Files.exists(cacheDirectory)) {
            try {
                Files.createDirectories(cacheDirectory);
            } catch (IOException e) {
                throw CacheSQLException.of("Cannot create the cache directory: " + cacheDirectory, e);
            }
        }
        if (!Files.isDirectory(cacheDirectory))
            throw CacheSQLException
                    .of("The path is not a directory, or the directory cannot be created: " + cacheDirectory);
        this.cacheDirectory = cacheDirectory;
        this.activeKeys = new ConcurrentHashMap<>();
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
    final CachedResultSet get(final CachedStatement statement, final String key, final Provider resultSetProvider)
            throws SQLException {
        final Path resultSetPath = cacheDirectory.resolve(key);
        if (!Files.exists(resultSetPath)) {
            if (resultSetProvider == null)
                throw new SQLException("No cache available");
            buildCache(key, resultSetPath, resultSetProvider);
        }
        return new CachedResultSet(statement, resultSetPath);
    }

    private void buildCache(final String key, final Path resultSetPath, final Provider resultSetProvider)
            throws SQLException {
        final Lock keyLock = activeKeys.computeIfAbsent(key, s -> new ReentrantLock(true));
        try {
            keyLock.lock();
            try {
                final Path tempPath = cacheDirectory.resolve(key + ".tmp");
                try {
                    final ResultSet providedResultSet = resultSetProvider.provide();
                    ResultSetWriter.write(tempPath, providedResultSet);
                    Files.move(tempPath, resultSetPath, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    throw new SQLException("Failed in renaming the file " + tempPath, e);

                } finally {
                    try {
                        Files.deleteIfExists(tempPath);
                    } catch (IOException e) {
                        // Quiet
                    }
                }
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

    final boolean checkIfExists(final String key) {
        final Path resultSetPath = cacheDirectory.resolve(key);
        return Files.exists(resultSetPath);
    }

    interface Provider {

        ResultSet provide() throws SQLException;
    }

}
