/**
 * Copyright 2016 Emmanuel Keller / QWAZR
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.statewidesoftware.jdbc.cache

import org.junit.BeforeClass
import java.io.File
import java.io.IOException
import java.nio.file.Files

abstract class OnDiskCacheJdbcWithDerbyBackendTest : JdbcWithDerbyBackendTest() {
    public override fun getOrSetJdbcCacheUrl(): String {
        return "jdbc:cache:file:" + tempDirPath + File.separatorChar + "com/statewidesoftware/jdbc/cache"
    }

    companion object {
        private var tempDirPath: String? = Files.createTempDirectory("jdbc-cache-test").toUri().path
        @BeforeClass
        @Throws(IOException::class)
        fun createTmpDir() {
            if (tempDirPath == null) {
                throw IOException("Cannot create temp directory")
            }
            tempDirPath?.let {
                if (it.contains(":") && it.startsWith("/")) {
                    tempDirPath = it.substring(1)
                }
            }
        }
    }
}