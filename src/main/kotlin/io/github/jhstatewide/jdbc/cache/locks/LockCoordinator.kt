package io.github.jhstatewide.jdbc.cache.locks

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class LockCoordinator {
    private val locks = ConcurrentHashMap<String, LockEntry>()

    @Throws(IllegalArgumentException::class)
    fun acquireLock(name: String) {
        val entry = locks.compute(name) { _, oldValue ->
            if (oldValue == null || oldValue.lock.isLocked.not()) {
                LockEntry(ReentrantLock(), 1)
            } else {
                oldValue.count++
                oldValue
            }
        } ?: throw IllegalArgumentException("Lock not found: $name")
        entry.lock.lock()
    }

    @Throws(IllegalArgumentException::class)
    fun releaseLock(name: String) {
        val entry = locks[name] ?: throw IllegalArgumentException("Lock not found: $name")
        synchronized(entry) {
            entry.count--
            if (entry.count == 0) {
                locks.remove(name, entry)
                entry.lock.unlock()
            }
        }
    }

    @Throws(IllegalArgumentException::class)
    fun <T> withLock(name: String, action: () -> T): T {
        val entry = locks.compute(name) { _, oldValue ->
            if (oldValue == null || oldValue.lock.isLocked.not()) {
                LockEntry(ReentrantLock(), 1)
            } else {
                oldValue.count++
                oldValue
            }
        } ?: throw IllegalArgumentException("Lock not found: $name")

        val result = entry.lock.withLock(action)
        synchronized(entry) {
            entry.count--
            if (entry.count == 0) {
                locks.remove(name, entry)
            }
        }
        return result
    }

    @Throws(IllegalArgumentException::class)
    fun withLock(name: String, action: () -> Unit) {
        val entry = locks.compute(name) { _, oldValue ->
            if (oldValue == null || oldValue.lock.isLocked.not()) {
                LockEntry(ReentrantLock(), 1)
            } else {
                oldValue.count++
                oldValue
            }
        } ?: throw IllegalArgumentException("Lock not found: $name")

        entry.lock.withLock(action)
        synchronized(entry) {
            entry.count--
            if (entry.count == 0) {
                locks.remove(name, entry)
            }
        }
    }

    fun withLock(lock: ReentrantLock, action: () -> Unit) {
        lock.withLock(action)
    }

    private data class LockEntry(val lock: ReentrantLock, var count: Int)
}
