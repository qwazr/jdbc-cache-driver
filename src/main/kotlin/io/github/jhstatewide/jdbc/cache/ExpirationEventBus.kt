package io.github.jhstatewide.jdbc.cache

import java.lang.ref.WeakReference

object ExpirationEventBus {
    private val listeners = mutableSetOf<WeakReference<ExpirationListener>>()

    private val logger = java.util.logging.Logger.getLogger(ExpirationEventBus::class.java.name)

    fun keyExpired(key: String) {
        notifyExpiration(key)
    }

    private fun notifyExpiration(key: String) {
        listeners.forEach { listener ->
            val listener = listener?.get()
            logger.finer("Notifying listener of expiration for key: $key")
            listener?.onExpiration(key)
        }
    }

    fun addListener(listener: ExpirationListener) {
        listeners.add(WeakReference(listener))
    }
}