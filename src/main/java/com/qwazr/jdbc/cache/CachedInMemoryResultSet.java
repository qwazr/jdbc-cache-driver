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
package com.qwazr.jdbc.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.sql.SQLException;

/**
 * Uses ByteArrayInputStream/ByteArrayOutputStream as the storage implementation.
 * Everything is hold in memory.
 * Warning: this is a super naive implementation. Not designed to run in production
 * as lot of memory is going to be used by converting to byte[].
 */
class CachedInMemoryResultSet extends CachedResultSet {
    CachedInMemoryResultSet(final CachedStatement statement, byte[] bytes) throws SQLException {
        super(statement, new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
