package com.statewidesoftware.jdbc.cache/*
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

import com.statewidesoftware.jdbc.cache.CacheException.Companion.of
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import java.util.logging.Level
import java.util.logging.Logger

internal class ResultSetOnDiskCacheImpl(cacheDirectory: Path) : ResultSetCacheImpl() {
    private val cacheDirectory: Path
    private val activeKeys: ConcurrentHashMap<String?, ReentrantLock>
    private val logger = Logger.getLogger(ResultSetOnDiskCacheImpl::class.java.name).apply { level = Level.ALL }

    init {
        logger.fine { "Using disk cache: $cacheDirectory" }
        if (!Files.exists(cacheDirectory)) {
            try {
                Files.createDirectories(cacheDirectory)
            } catch (e: IOException) {
                throw of("Cannot create the cache directory: $cacheDirectory", e)
            }
        }
        if (!Files.isDirectory(cacheDirectory)) throw of("The path is not a directory, or the directory cannot be created: $cacheDirectory")
        this.cacheDirectory = cacheDirectory
        activeKeys = ConcurrentHashMap()
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
    @Throws(SQLException::class)
    override operator fun <T> get(
        statement: CachedStatement<*>?,
        key: String?,
        resultSetProvider: ResultSetCache.Provider?
    ): CachedOnDiskResultSet? {
        val resultSetPath = cacheDirectory.resolve(key)
        if (!Files.exists(resultSetPath)) {
            if (resultSetProvider == null) throw SQLException("No cache available")
            buildCache(key, resultSetPath, resultSetProvider)
        }
        return try {
            if (statement == null) return null
            CachedOnDiskResultSet(statement, resultSetPath)
        } catch (e: IOException) {
            throw SQLException("Can not read cache", e)
        }
    }

    @Throws(SQLException::class)
    private fun buildCache(key: String?, resultSetPath: Path, resultSetProvider: ResultSetCache.Provider) {
        val keyLock: Lock = activeKeys.computeIfAbsent(key) { s: String? -> ReentrantLock(true) }
        try {
            keyLock.lock()
            try {
                val tempPath = cacheDirectory.resolve("$key.tmp")
                try {
                    val providedResultSet = resultSetProvider.provide()
                    if (providedResultSet == null) {
                        Files.deleteIfExists(tempPath)
                        return
                    }
                    ResultSetWriter.write(tempPath, providedResultSet)
                    Files.move(
                        tempPath, resultSetPath, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                    )
                } catch (e: IOException) {
                    throw SQLException("Failed in renaming the file $tempPath", e)
                } finally {
                    try {
                        Files.deleteIfExists(tempPath)
                    } catch (e: IOException) {
                        // Quiet
                    }
                }
            } finally {
                keyLock.unlock()
            }
        } finally {
            activeKeys.remove(key)
        }
    }

    /**
     * Check if an entry is available for this key.
     *
     * @param key the computed key
     * @return always true if the cache entry exists
     */
    override fun checkIfExists(key: String?): Boolean {
        val resultSetPath = cacheDirectory.resolve(key)
        return Files.exists(resultSetPath)
    }

    @Throws(SQLException::class)
    private fun parse(consumer: Consumer<Path>) {
        try {
            Files.list(cacheDirectory).use { stream ->
                stream.forEach { path: Path ->
                    val name = path.fileName.toString()
                    if (!name.endsWith(".tmp")) {
                        val keyLock: Lock = activeKeys.computeIfAbsent(name) { s: String? -> ReentrantLock(true) }
                        try {
                            keyLock.lock()
                            try {
                                consumer.accept(path)
                            } finally {
                                keyLock.unlock()
                            }
                        } finally {
                            activeKeys.remove(name)
                        }
                    }
                }
            }
        } catch (e: CacheException) {
            throw e.sQLException
        } catch (e: IOException) {
            throw of(e).sQLException
        }
    }

    @Throws(SQLException::class)
    override fun flush() {
        parse { path: Path? ->
            try {
                Files.deleteIfExists(path)
            } catch (e: IOException) {
                throw of(e)
            }
        }
    }

    private fun checkCacheDirectory(): Path {
        return Objects.requireNonNull(cacheDirectory, "No cache directory")
    }

    @Throws(SQLException::class)
    override fun flush(stmt: Statement?) {
        try {
            Files.deleteIfExists(cacheDirectory.resolve(checkKey(stmt)))
        } catch (e: IOException) {
            throw of(e)
        }
    }

    @Throws(SQLException::class)
    override fun size(): Int {
        val counter = AtomicInteger()
        parse { path: Path -> if (!path.endsWith(".tmp")) counter.incrementAndGet() }
        return counter.get()
    }

    @Throws(SQLException::class)
    override fun exists(stmt: Statement?): Boolean {
        return Files.exists(checkCacheDirectory().resolve(checkKey(stmt)))
    }
}