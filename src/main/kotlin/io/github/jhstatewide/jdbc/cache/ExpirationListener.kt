package io.github.jhstatewide.jdbc.cache

interface ExpirationListener {
    fun onExpiration(key: String)
}