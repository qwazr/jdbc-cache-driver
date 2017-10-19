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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.logging.Logger;

public abstract class JdbcTest {
    final static Logger LOGGER = Logger.getLogger(JdbcTest.class.getName());

    abstract String getOrSetJdbcCacheUrl();
    abstract Connection getConnection() throws SQLException;

    @BeforeClass
    public static void initDriver() throws ClassNotFoundException, IOException {
        Class.forName("com.qwazr.jdbc.cache.Driver");
    }

    Connection getNoBackendConnection() throws SQLException {
        return DriverManager.getConnection(getOrSetJdbcCacheUrl());
    }

    @Test
    public void test000testDriver() throws SQLException {
        java.sql.Driver driver = DriverManager.getDriver(getOrSetJdbcCacheUrl());
        Assert.assertNotNull(driver);
        Assert.assertEquals(1, driver.getMajorVersion());
        Assert.assertEquals(3, driver.getMinorVersion());
        Assert.assertNotNull(driver.getParentLogger());
        Assert.assertFalse(driver.jdbcCompliant());
        DriverPropertyInfo[] infos = driver.getPropertyInfo(null, null);
        Assert.assertNotNull(infos);
        Assert.assertEquals(3, infos.length);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_URL, infos[0].name);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_CLASS, infos[1].name);
        Assert.assertEquals(com.qwazr.jdbc.cache.Driver.CACHE_DRIVER_ACTIVE, infos[2].name);
    }

    @Test
    public void test002initConnection() throws SQLException {
        Assert.assertNotNull(getConnection());
        Assert.assertNotNull(getNoBackendConnection());
    }
}
