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

import java.sql.Driver;
import java.sql.DriverAction;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class DriverMap implements DriverAction {

    private final Map<Class<? extends Driver>, Driver> drivers;

    private volatile Map<Class<? extends Driver>, Driver> cache;

    DriverMap() {
        drivers = new HashMap<>();
        cache = new HashMap<>();
    }

    /**
     * Retrieve the driver for the given driverClassName.
     *
     * @param driverClassName the class name of the driver
     * @return an instance of the driver
     * @throws ReflectiveOperationException
     */
    final Driver findDriver(final String driverClassName) throws ReflectiveOperationException {

        // First we try the cache
        Driver driver = cache.get(driverClassName);
        if (driver != null)
            return driver;

        synchronized (drivers) {
            final Class<? extends Driver> classDriver = (Class<? extends Driver>) Class.forName(driverClassName);
            driver = drivers.get(classDriver);
            if (driver != null)
                return driver;
            driver = classDriver.newInstance();
            drivers.put(classDriver, driver);
            cache = new HashMap<>(drivers);
            return driver;
        }
    }

    @Override
    public void deregister() {
        synchronized (drivers) {
            drivers.forEach((aClass, driver) -> {
                if (driver instanceof DriverAction)
                    ((DriverAction) driver).deregister();
            });
            drivers.clear();
            cache.clear();
        }
    }

    boolean acceptsURL(final String url) throws SQLException {
        Collection<Driver> collection = cache.values();
        for (Driver driver : collection)
            if (driver.acceptsURL(url))
                return true;
        return false;
    }

    void fillPropertyInfo(final String url, final Properties info, final Collection<DriverPropertyInfo> properties)
            throws SQLException {
        Collection<Driver> collection = cache.values();
        for (Driver driver : collection) {
            DriverPropertyInfo[] driverInfos = driver.getPropertyInfo(url, info);
            if (driverInfos != null)
                for (DriverPropertyInfo driverInfo : driverInfos)
                    properties.add(driverInfo);
        }
    }
}
