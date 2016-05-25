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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;

class ResultSetCache {

    private final Path cacheDirectory;

    ResultSetCache(final Path cacheDirectory) throws SQLException {
        if (!Files.exists(cacheDirectory)) {
            try {
                Files.createDirectory(cacheDirectory);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        if (!Files.isDirectory(cacheDirectory))
            throw new SQLException(
                    "The path is not a directory, or the directory cannot be created: " + cacheDirectory);
        this.cacheDirectory = cacheDirectory;
    }

    CachedResultSet get(final ResultSetKey key, Provider resultSetProvider) throws SQLException {
        final Path resultSetPath = cacheDirectory.resolve(key.getFileName());
        final ResultSet providedResultSet;
        if (!Files.exists(resultSetPath)) {
            providedResultSet = resultSetProvider.provide();
            ResultSetWriter.write(resultSetPath, providedResultSet);
        }
        return new CachedResultSet(resultSetPath);
    }

    interface Provider {

        ResultSet provide() throws SQLException;
    }
}
