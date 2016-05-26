# jdbc-cache-driver

[![Build Status](https://travis-ci.org/qwazr/jdbc-cache-driver.svg?branch=master)](https://travis-ci.org/qwazr/jdbc-cache-driver)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver)
[![Join the chat at https://gitter.im/qwazr/jdbc-cache-driver](https://badges.gitter.im/qwazr/jdbc-cache-driver.svg)](https://gitter.im/qwazr/jdbc-cache-driver)


JDBC-Driver-Cache is JDBC cache which store the result of a SQL query (ResultSet) in a file.
The same query requested again will be read from the file, the database is no more requested again.

JDBC-Driver-Cache is itself a JDBC driver and acts as a wrapper over any third-party JDBC driver.

Usage
-----

### Add the driver in your maven projet

The library is available on Maven Central.

```xml
<dependency>
  <groupId>com.qwazr</groupId>
  <artifactId>jdbc-cache-driver</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

### JAVA Code example

First, you have to initialize the JDBC drivers.
In this example we use Apache Derby as backend driver.
You can use any compliant JDBC driver.

```java
// Initialize the cache driver
Class.forName("com.qwazr.jdbc.cache.Driver");

// Initialize the third-party driver
Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

// Provide the URL of the backend driver
Properties info = new Properties();
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");

// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

To build a connection you have to provide the URL and some properties.
The URL tells the driver where to store the cached ResultSet.

The syntax of the URL is:

*jdbc:cache:file:{path-to-the-cache-directory}*

The property **cache.driver.url** contains the typical JDBC URL of the backend driver.
The properties are passed to both the cache driver and the backend driver.

### Use in transparent mode

You can also disable the cache by setting **false** to the property **cache.driver.active**.
In this mode, the cache driver is transparent. All the queries and the result handled by the backend-driver.

```java
info.setProperty("cache.driver.active", "false");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

Community
---------

JDBC-Driver-Cache is open source and is licensed under the Apache 2.0 License.

Report any issue here:
https://github.com/qwazr/jdbc-cache-driver/issues
