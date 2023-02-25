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
package com.statewidesoftware.jdbc.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class OnDiskCacheEnabledJdbcWithDerbyBackendTest extends OnDiskCacheJdbcWithDerbyBackendTest {
    @Override
    Class<? extends ResultSet> expectedResultSetClass() {
        return CachedOnDiskResultSet.class;
    }

    @Override
    boolean isCacheEnabled() {
        return true;
    }

    @Override
    String getDerbyDbName() {
        return "onDiskCacheEnabled";
    }

    @Override
    Connection getConnection() throws SQLException {
        final Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:derby:memory:" + getDerbyDbName() + ";create=true");
        return DriverManager.getConnection(getOrSetJdbcCacheUrl(), info);
    }
}