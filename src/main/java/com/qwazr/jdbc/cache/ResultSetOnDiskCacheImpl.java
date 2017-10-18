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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

class ResultSetOnDiskCacheImpl extends ResultSetCacheImpl {

    private final Path cacheDirectory;
    private final ConcurrentHashMap<String, ReentrantLock> activeKeys;

    ResultSetOnDiskCacheImpl(final Path cacheDirectory) {
        if (!Files.exists(cacheDirectory)) {
            try {
                Files.createDirectories(cacheDirectory);
            } catch (IOException e) {
                throw CacheException.of("Cannot create the cache directory: " + cacheDirectory, e);
            }
        }
        if (!Files.isDirectory(cacheDirectory))
            throw CacheException
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
    public CachedOnDiskResultSet get(final CachedStatement statement, final String key, final Provider resultSetProvider)
            throws SQLException {
        final Path resultSetPath = cacheDirectory.resolve(key);
        if (!Files.exists(resultSetPath)) {
            if (resultSetProvider == null)
                throw new SQLException("No cache available");
            buildCache(key, resultSetPath, resultSetProvider);
        }
        try {
            return new CachedOnDiskResultSet(statement, resultSetPath);
        } catch (IOException e) {
            throw new SQLException("Can not read cache", e);
        }
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

    public boolean checkIfExists(final String key) {
        final Path resultSetPath = cacheDirectory.resolve(key);
        return Files.exists(resultSetPath);
    }

    private void parse(final Consumer<Path> consumer) throws SQLException {
        try {
            synchronized (cacheDirectory) {
                Files.list(cacheDirectory).forEach(path -> {
                    if (!path.endsWith(".tmp"))
                        consumer.accept(path);
                });
            }
        } catch (CacheException e) {
            throw e.getSQLException();
        } catch (IOException e) {
            throw CacheException.of(e).getSQLException();
        }
    }

    @Override
    public void flush() throws SQLException {
        parse(path -> {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                throw CacheException.of(e);
            }
        });
    }

    private Path checkCacheDirectory() {
        return Objects.requireNonNull(cacheDirectory, "No cache directory");
    }

    @Override
    public void flush(final Statement stmt) throws SQLException {
        try {
            Files.deleteIfExists(cacheDirectory.resolve(checkKey(stmt)));
        } catch (IOException e) {
            throw CacheException.of(e);
        }
    }

    @Override
    public int size() throws SQLException {
        final AtomicInteger counter = new AtomicInteger();
        parse(path -> {
            if (!path.endsWith(".tmp"))
                counter.incrementAndGet();
        });
        return counter.get();
    }

    @Override
    public boolean exists(Statement stmt) throws SQLException {
        return Files.exists(checkCacheDirectory().resolve(checkKey(stmt)));
    }
}
