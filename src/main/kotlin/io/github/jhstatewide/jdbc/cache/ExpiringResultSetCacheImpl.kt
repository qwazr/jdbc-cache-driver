package io.github.jhstatewide.jdbc.cache

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger


internal class ExpiringResultSetCacheImpl(cacheDirectory: Path) : ExpiringResultSetCache, ResultSetOnDiskCacheImpl(
    cacheDirectory
) {

    var _maxSize: Int? = null
    var _maxAge: Duration? = null

    val garbageCollectionLock = ReentrantLock()

    constructor(cacheDirectory: Path, maxSize: Int?, maxAge: Duration?) : this(cacheDirectory) {
        this._maxSize = maxSize
        this._maxAge = maxAge
    }

    private val defaultGarbageCollectionInterval: Duration = Duration.ofHours(1)

    private val logger = Logger.getLogger(ExpiringResultSetCacheImpl::class.java.name)

    private var garbageCollectionInterval: Duration = defaultGarbageCollectionInterval

    private val garbageCollectionClosure = {
        while (true) {
            Thread.sleep(defaultGarbageCollectionInterval.toMillis())
            garbageCollection()
        }
    }

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    init {
        // start a thread with the garbage collection closure
        Thread(garbageCollectionClosure).start()
    }

    override fun setMaxSize(maxSize: Int?) {
        this._maxSize = maxSize
    }

    override fun getMaxSize(): Int? {
        return this._maxSize
    }

    override fun setGcInterval(gcInterval: Long) {
        this.garbageCollectionInterval = Duration.ofMillis(gcInterval)
    }

    override fun setMaxAge(maxAge: Duration?) {
        this._maxAge = maxAge
    }

    private fun garbageCollection() {
        // use the lock to prevent multiple threads from running garbage collection at the same time
        garbageCollectionLock.lock()

        try {
            // look at each file in the cache directory
            jdbcCacheFiles()
                .forEach { file ->
                    // if the file is older than the max age, delete it
                    if (Duration.ofMillis(file.lastModified()).plus(_maxAge).isNegative) {
                        file.delete()
                    }
                }
            // if the cache is larger than the max size, delete the oldest file
            if (isAboveMaxSize()) {
                // get the oldest file
                val oldestFile = jdbcCacheFiles()
                    .minByOrNull { it.lastModified() }
                // delete the oldest file
                oldestFile?.delete()
            }
        } finally {
            garbageCollectionLock.unlock()
        }
    }

    private fun isAboveMaxSize(): Boolean {
        getMaxSize()?.let { currentMaxSize ->
            return size() > currentMaxSize
        }
        return false
    }

    private fun jdbcCacheFiles() = this.cacheDirectory.toFile()
        .listFiles()
        ?.filterNotNull()
        // filter by files that end in the JDBC cache file extension
        ?.filter { it.name.endsWith(JDBC_CACHE_EXTENSION) } ?: emptyList()

    // overwrite set because if we are above the max size, we need to garbage collect
    override fun <T> get(
        statement: CachedStatement<*>?,
        key: String?,
        resultSetProvider: ResultSetCache.Provider?
    ): CachedOnDiskResultSet? {
        try {
            possiblyPurge(key)
        } catch (e: Exception) {
            logger.warning("Error purging cache file: ${e.localizedMessage}")
        }

        return super.get<T>(statement, key, resultSetProvider).also {
            // if GC is not running, start it
            if (!this.garbageCollectionLock.isLocked) {
                coroutineScope.launch {
                    if (isAboveMaxSize()) {
                        garbageCollection()
                    }
                }
            }
        }
    }

    private fun possiblyPurge(key: String?) {
        // get the creation time of the file
        val lastModifiedTime = key?.let { this.cacheDirectory.resolve(it).toFile().lastModified() }
        // determine if the file is older than the max age
        val isOlderThanMaxAge = lastModifiedTime?.let { Duration.ofMillis(it).plus(_maxAge).isNegative }

        if (isOlderThanMaxAge == true) {
            // delete the file
            this.cacheDirectory.resolve(key).toFile().delete()
        }
    }

}