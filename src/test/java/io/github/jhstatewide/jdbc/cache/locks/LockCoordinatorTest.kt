package io.github.jhstatewide.jdbc.cache.locks


import junit.framework.TestCase.assertEquals
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun assertThrows(expectedException: Class<out Throwable>, block: () -> Unit) {
    try {
        block()
    } catch (e: Throwable) {
        if (expectedException.isInstance(e)) {
            return
        }
        throw e
    }
    throw AssertionError("Expected exception of type ${expectedException.name} but no exception was thrown")
}

class LockCoordinatorTest {
    private val lockCoordinator = LockCoordinator()

    @Test
    fun `acquireLock and releaseLock should work correctly`() {
        lockCoordinator.acquireLock("testLock")
        lockCoordinator.releaseLock("testLock")
    }

    @Test
    fun `releasing a lock that doesn't exist should throw an exception`() {
        assertThrows(IllegalArgumentException::class.java) { lockCoordinator.releaseLock("nonExistentLock") }
    }

    @Test
    fun `withLock should work correctly`() {
        lockCoordinator.withLock("testLock") {
            // code to be executed while holding the lock
        }
    }

    @Test
    fun `withLock should automatically release the lock after execution`() {
        lockCoordinator.withLock("testLock") {
            // code to be executed while holding the lock
        }
        assertThrows(IllegalArgumentException::class.java) { lockCoordinator.releaseLock("testLock") }
    }

    @Test
    fun `withLock should handle multiple threads accessing the same lock`() {
        val executor = Executors.newFixedThreadPool(10)
        val latch = CountDownLatch(10)

        for (i in 1..10) {
            executor.submit {
                lockCoordinator.withLock("testLock") {
                    // code to be executed while holding the lock
                }
                latch.countDown()
            }
        }

        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.MINUTES)
        assertEquals(0, latch.count)
    }

}
