/**
 * Copyright 2016 Emmanuel Keller / QWAZR
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
package com.qwazr.jdbc.cache;

import java.io.*;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

class ClobString implements Clob {

    final String content;

    ClobString(String content) {
        this.content = content;
    }

    @Override
    public long length() throws SQLException {
        return content == null ? 0 : content.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return content == null ? null : content.substring((int) pos - 1, length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return content == null ? null : new StringReader(content);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return content == null ? null : new ByteArrayInputStream(content.getBytes());
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        if (content == null)
            return -1;
        final int pos = content.indexOf(searchstr, (int) start - 1);
        return pos == -1 ? pos : pos + 1;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(content.substring((int) pos - 1, (int) (pos + length - 1)));
    }
}
