# jdbc-cache-driver

[![Build Status](https://travis-ci.org/qwazr/jdbc-cache-driver.svg?branch=master)](https://travis-ci.org/qwazr/jdbc-cache-driver)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver)
[![Join the chat at https://gitter.im/qwazr/jdbc-cache-driver](https://badges.gitter.im/qwazr/jdbc-cache-driver.svg)](https://gitter.im/qwazr/jdbc-cache-driver)

How it works
------------

JDBC-Driver-Cache is JDBC driver which store the result of a SQL query (ResultSet) in a file.
If you request the same query again, the database is no more requested.
The drive uses the cached ResultSet.

JDBC-Driver-Cache acts as a wrapper over any third-party JDBC driver.
It uses this driver as backend to first retrieve the ResultSet.


Usage
-----

The library is available on Maven Central.

```xml
<dependency>
  <groupId>com.qwazr</groupId>
  <artifactId>jdbc-cache-driver</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

First, you have to initialize the JDBC drivers.
In this example we use Apache Derby as backend driver.
You can use any compliant JDBC driver.

```java

// Initialize the driver of the cache
Class.forName("com.qwazr.jdbc.cache.Driver");

// Initialize the third-party driver
Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

```

To build a connection you have to provide an URL and properties.
The URL tells the driver where to store the cached ResultSet.
Thx syntax of the URL is:

*jdbc:cache:file:{path-to-the-cache-directory}*

The property **cache.driver.url** contains the typical JDBC URL of the backend driver.
The properties are passed to both the cache driver and the backend driver.

```java

Properties info = new Properties();
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);

```

You can also disable the cache by setting **false** to the property **cache.driver.active**.
In this mode, the cache driver is transparent. All the queries and the result handled by the backend-driver.

```java

info2.setProperty("cache.driver.active", "false");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);

```

Issues
------

Report any issue here:
https://github.com/qwazr/jdbc-cache-driver/issues