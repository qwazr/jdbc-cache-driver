# kotlin-jdbc-cache-driver

## Build Status
![Build](https://github.com/jhstatewide/kotlin-jdbc-cache-driver/actions/workflows/build.yml/badge.svg)

## Note

***This was forked and ported to Kotlin from the original Java project at
https://github.com/qwazr/jdbc-cache-driver***

## Overview

Kotlin-JDBC-Driver-Cache is JDBC cache which store the result of a SQL query (ResultSet) in files or in memory.
The same query requested again will be read from the file, the database is no more requested again.

You may use it to easily mock ResultSets from a database.

Kotlin-JDBC-Driver-Cache is itself a JDBC driver and acts as a wrapper over any third-party JDBC driver.

Usage
-----

### ~~Add the driver in your maven project~~

~~The library is available on Maven Central.~~


~~~strike
<dependency>
  <groupId>com.statewidesoftware</groupId>
  <artifactId>kotlin-jdbc-cache-driver</artifactId>
  <version>1.4</version>
</dependency>
~~~

**NOTE**: I have not yet published this to Maven Central. Waiting for access... in the meantime jars are published
under "Releases" on GitHub, or of course you can build it yourself.

### JAVA Code example

First, you have to initialize the JDBC drivers.
In this example we use Apache Derby as backend driver.
You can use any compliant JDBC driver.

```java
// Initialize the cache driver
Class.forName("com.statewidesoftware.jdbc.cache.Driver");

// Provide the URL and the Class name of the backend driver
Properties info = new Properties();
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
info.setProperty("cache.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
```

Use the file cache implementation:

```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

Or use the in memory cache implementation:

```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:mem:my-memory-cache", info);
```

To build a connection you have to provide the URL and some properties.
The URL tells the driver where to store the cached ResultSet.

The syntax of the URL can be:

* *jdbc:cache:file:{path-to-the-cache-directory}* for on disk cache
* *jdbc:cache:mem:{name-of-the-cache}* for in memory cache

Two possible properties:
- **cache.driver.url** contains the typical JDBC URL of the backend driver.
- **cache.driver.class** contains the class name of the backend driver.

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

Kotlin JDBC-Driver-Cache is open source and is licensed under the Apache 2.0 License.

Report any issue here:
https://github.com/jhstatewide/jdbc-cache-driver/issues
