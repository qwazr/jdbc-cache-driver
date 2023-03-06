package io.github.jhstatewide.jdbc.cache

import java.time.Duration

interface ExpiringResultSetCache: ResultSetCache {
    fun setMaxSize(maxSize: Int?)
    fun getMaxSize(): Int?

    // garbage collection interval
    fun setGcInterval(gcInterval: Long)

    // maximum time a cache entry can live
    fun setMaxAge(maxAge: Duration?)
}