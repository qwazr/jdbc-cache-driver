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

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.SortedMap;

class ResultSetCache {

    private final Path cacheDirectory;

    ResultSetCache(final Path cacheDirectory) throws SQLException {
        if (!Files.exists(cacheDirectory)) {
            try {
                Files.createDirectories(cacheDirectory);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        if (!Files.isDirectory(cacheDirectory))
            throw new SQLException(
                    "The path is not a directory, or the directory cannot be created: " + cacheDirectory);
        this.cacheDirectory = cacheDirectory;
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
        final ResultSet providedResultSet;
        if (!Files.exists(resultSetPath)) {
            if (resultSetProvider == null)
                throw new SQLException("No cache available");
            final Path tempPath = cacheDirectory.resolve(key + "." + System.currentTimeMillis() + ".tmp");
            try {
                providedResultSet = resultSetProvider.provide();
                ResultSetWriter.write(tempPath, providedResultSet);
                Files.move(tempPath, resultSetPath);
            } catch (IOException e) {
                throw new SQLException("Failed in renaming the file " + tempPath, e);
            } finally {
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException e) {
                    // Quiet
                }
            }
        }
        return new CachedResultSet(statement, resultSetPath);
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

    private static String digest(final String src) throws SQLException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return DatatypeConverter.printHexBinary(md.digest(src.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new SQLException("MD5 is not available");
        }
    }

    final static String getKey(final String sql) throws SQLException {
        return digest(sql);
    }

    final static String getKey(final String sql, final SortedMap<Integer, Object> parameters) throws SQLException {
        final StringBuilder sb = new StringBuilder(sql);
        parameters.forEach((index, value) -> {
            sb.append('•');
            sb.append(index);
            sb.append(value.toString());
        });
        return digest(sb.toString());
    }
}
