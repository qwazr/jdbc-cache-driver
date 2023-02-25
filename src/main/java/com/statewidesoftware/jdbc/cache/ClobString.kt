/*
 * Copyright 2016-2017 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.statewidesoftware.jdbc.cache

import java.io.*
import java.sql.Clob
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException

internal class ClobString(val content: String?) : Clob {
    @Throws(SQLException::class)
    override fun length(): Long {
        return content?.length?.toLong() ?: 0
    }

    @Throws(SQLException::class)
    override fun getSubString(pos: Long, length: Int): String? {
        return content?.substring(pos.toInt() - 1, length)
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(): Reader? {
        return if (content == null) null else StringReader(content)
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(): InputStream? {
        return if (content == null) null else ByteArrayInputStream(content.toByteArray())
    }

    @Throws(SQLException::class)
    override fun position(searchstr: String, start: Long): Long {
        if (content == null) return -1
        val pos = content.indexOf(searchstr, start.toInt() - 1)
        return if (pos == -1) pos.toLong() else (pos + 1).toLong()
    }

    @Throws(SQLException::class)
    override fun position(searchstr: Clob, start: Long): Long {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setString(pos: Long, str: String): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setString(pos: Long, str: String, offset: Int, len: Int): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setAsciiStream(pos: Long): OutputStream {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setCharacterStream(pos: Long): Writer {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun truncate(len: Long) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun free() {
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(pos: Long, length: Long): Reader {
        return StringReader(content!!.substring(pos.toInt() - 1, (pos + length - 1).toInt()))
    }
}