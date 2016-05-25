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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcCacheDriver implements Driver, DriverAction {

    final static Logger LOGGER = Logger.getLogger(JdbcCacheDriver.class.getPackage().getName());

    public final static String BACKEND_CLASSNAME_PROPERTY = "cachedriver.backend.classname";
    public final static String CACHE_DIRECTORY = "cachedriver.directory";

    private final DriverMap drivers = new DriverMap();

    public Connection connect(String url, Properties info) throws SQLException {

        // Load the optional properties
        final String backendClassName;
        final String cacheDirectoryPath;

        if (info != null) {
            backendClassName = info.getProperty(BACKEND_CLASSNAME_PROPERTY);
            cacheDirectoryPath = info.getProperty(CACHE_DIRECTORY);
        } else {
            backendClassName = null;
            cacheDirectoryPath = null;
        }

        // Determine the backend connection
        final Connection backendConnection;
        if (backendClassName != null) {
            try {
                backendConnection = drivers.findDriver(backendClassName).connect(url, info);
            } catch (ReflectiveOperationException e) {
                throw new SQLException("Cannot initialize the backend JDBC driver: " + backendClassName, e);
            }
        } else
            backendConnection = null;

        // Locate the cache directory
        final Path cacheDirectory;
        if (cacheDirectoryPath != null)
            cacheDirectory = FileSystems.getDefault().getPath(cacheDirectoryPath);
        else
            cacheDirectory = null;

        return new CachedConnection(backendConnection, cacheDirectory);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return drivers.acceptsURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
        Collection<DriverPropertyInfo> properties = new ArrayList<>();
        properties.add(new DriverPropertyInfo(BACKEND_CLASSNAME_PROPERTY, null));
        properties.add(new DriverPropertyInfo(CACHE_DIRECTORY, null));
        // Retrieve the properties from all the backend drivers
        drivers.fillPropertyInfo(url, info, properties);
        return properties.toArray(new DriverPropertyInfo[properties.size()]);
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

    @Override
    public void deregister() {
        drivers.deregister();
    }
}
