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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

class Driver implements java.sql.Driver {

    final static Logger LOGGER = Logger.getLogger(Driver.class.getPackage().getName());

    private final static String URL_PREFIX = "jdbc:cache:file:";
    private final static String CACHE_DRIVER_URL = "cache.driver.url";

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {

        // Determine the optional backend connection
        final String cacheDriverUrl = info.getProperty(CACHE_DRIVER_URL);
        final Connection backendConnection =
                cacheDriverUrl == null ? null : DriverManager.getConnection(cacheDriverUrl, info);

        if (url.length() <= URL_PREFIX.length())
            throw new SQLException("The path is empty: " + url);
        final Path cacheDirectory = FileSystems.getDefault().getPath(url.substring(URL_PREFIX.length()));
        return new CachedConnection(backendConnection, cacheDirectory);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(URL_PREFIX);
    }

    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
        return new DriverPropertyInfo[] { new DriverPropertyInfo(CACHE_DRIVER_URL, null) };
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }

}
