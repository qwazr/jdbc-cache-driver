# kotlin-jdbc-cache-driver

## Build Status
![Build](https://github.com/jhstatewide/kotlin-jdbc-cache-driver/actions/workflows/build.yml/badge.svg)

## Note

***This was forked and ported to Kotlin from the original Java project at
https://github.com/qwazr/jdbc-cache-driver***

## Overview

Kotlin-JDBC-Driver-Cache is a tool designed to cache the result of SQL queries, specifically the ResultSet, either in memory or files. With this caching functionality, subsequent requests for the same query can be served directly from the cache without the need for repeated database queries, resulting in improved performance and reduced load on the database.

This tool can also be used to mock ResultSets from a database, making it easier to test and develop database-driven applications. Additionally, Kotlin-JDBC-Driver-Cache is itself a JDBC driver that acts as a wrapper over any third-party JDBC driver.

By leveraging Kotlin-JDBC-Driver-Cache, developers can improve the performance and efficiency of their database-driven applications, while also simplifying the testing and development process.

## FAQ

FAQ at: https://github.com/jhstatewide/kotlin-jdbc-cache-driver/wiki/FAQ

## Roadmap
In addition to its existing features, Kotlin-JDBC-Driver-Cache is also constantly evolving and improving. One of the next steps in its development is to add more customization options, including the ability to set a Time-To-Live (TTL) value for cached data. This would allow developers to specify how long data should remain cached before being refreshed or invalidated, providing greater control over the caching behavior.

Furthermore, Kotlin-JDBC-Driver-Cache is also exploring support for standard "JSR-107" caching, which is a widely-used caching standard in the Java ecosystem. By implementing this standard, Kotlin-JDBC-Driver-Cache could be seamlessly integrated with other JSR-compliant caching solutions, making it even easier for developers to adopt and use.

Overall, Kotlin-JDBC-Driver-Cache is committed to providing a reliable, high-performance caching solution for JDBC-based applications, and will continue to evolve and improve to meet the changing needs of developers and businesses.

Usage
-----

### ~~Add the driver in your maven project~~

~~The library is available on Maven Central.~~


~~~strike
<dependency>
  <groupId>io.github.jhstatewide</groupId>
  <artifactId>kotlin-jdbc-cache-driver</artifactId>
  <version>1.4.7</version>
</dependency>
~~~

**NOTE**: I have not yet published this to Maven Central. Waiting for access... in the meantime jars are published
under "Releases" on GitHub, or of course you can build it yourself.

### Java / Kotlin Code example

First, you have to initialize the JDBC drivers.
In this example we use Apache Derby as backend driver.
You can use any compliant JDBC driver.

#### Java
```java
// Initialize the cache driver
Class.forName("io.github.jhstatewide.jdbc.cache.Driver");

// Provide the URL and the Class name of the backend driver
Properties info = new Properties();
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
info.setProperty("cache.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
```

#### Kotlin
```kotlin
// Initialize the cache driver
Class.forName("io.github.jhstatewide.jdbc.cache.Driver")

// Provide the URL and the Class name of the backend driver
val info = Properties()
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true")
info.setProperty("cache.driver.class", "org.apache.derby.jdbc.EmbeddedDriver")

```

Use the file cache implementation:

#### Java
```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

#### Kotlin
```kotlin
// Get your JDBC connection
val cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info)
```

Or use the in memory cache implementation:

#### Java
```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:mem:my-memory-cache", info);
```

#### Kotlin
```kotlin
// Get your JDBC connection
val cnx = DriverManager.getConnection("jdbc:cache:mem:my-memory-cache", info)
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

#### Java
```java
info.setProperty("cache.driver.active", "false");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

#### Kotlin
```kotlin
info.setProperty("cache.driver.active", "false")
val cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info)
```

### Expiration

You can set an expiration time for the cache. The expiration time is the time in seconds before the cache is invalidated.
The default value is 0, which means no expiration.
To set it use the property "cache.driver.max.age".

#### Java
```java
info.setProperty("cache.driver.max.age", "3600");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

#### Kotlin
```kotlin
info.setProperty("cache.driver.max.age", "3600")
val cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info)
```

or, choose to expire by maximum number of entries:

#### Java
```java
info.setProperty("cache.driver.max.size", "1000");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

#### Kotlin
```kotlin
info.setProperty("cache.driver.max.size", "1000")
val cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info)
```


Community
---------

Kotlin JDBC-Driver-Cache is open source and is licensed under the Apache 2.0 License.

Report any issue here:
https://github.com/jhstatewide/kotlin-jdbc-cache-driver/issues
