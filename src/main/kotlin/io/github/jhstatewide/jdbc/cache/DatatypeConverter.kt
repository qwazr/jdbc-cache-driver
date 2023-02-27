package io.github.jhstatewide.jdbc.cache

import java.nio.charset.StandardCharsets
import java.util.*

object DatatypeConverter {
    fun printHexBinary(data: ByteArray): String {
        val formatter = Formatter()
        for (b in data) {
            formatter.format("%02x", b)
        }
        val hex = formatter.toString()
        formatter.close()
        return hex
    }

    fun parseHexBinary(s: String): ByteArray {
        val len = s.length
        val data = ByteArray(len / 2)
        var i = 0
        while (i < len) {
            data[i / 2] = ((s[i].digitToIntOrNull(16) ?: (-1 shl 4)) + s[i + 1].digitToIntOrNull(16)!!).toByte()
            i += 2
        }
        return data
    }

    fun printBase64Binary(data: ByteArray?): String {
        return Base64.getEncoder().encodeToString(data)
    }

    fun parseBase64Binary(s: String?): ByteArray {
        return Base64.getDecoder().decode(s)
    }

    fun printHexBinary(s: String): String {
        return printHexBinary(s.toByteArray(StandardCharsets.UTF_8))
    }

    fun parseString(s: String): String {
        return parseHexBinary(s).toString(StandardCharsets.UTF_8)
    }
}