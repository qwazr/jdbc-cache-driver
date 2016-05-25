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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.zip.GZIPOutputStream;

class ResultSetWriter {

    final static void write(final Path resultSetPath, final ResultSet resultSet) throws SQLException {
        try (final FileOutputStream fos = new FileOutputStream(resultSetPath.toFile())) {
            try (final GZIPOutputStream zoz = new GZIPOutputStream(fos)) {
                try (ObjectOutputStream output = new ObjectOutputStream(zoz)) {
                    writeMetadata(output, resultSet.getMetaData());
                }
            }
        } catch (IOException e) {
            throw new SQLException("Error while writing the ResultSet cache file: " + resultSetPath, e);
        }
    }

    private static void writeMetadata(final ObjectOutputStream output, ResultSetMetaData metadata)
            throws IOException, SQLException {
        final int columnCount = metadata.getColumnCount();
        output.writeInt(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            output.writeUTF(metadata.getCatalogName(i));
            output.writeUTF(metadata.getColumnClassName(i));
            output.writeUTF(metadata.getColumnLabel(i));
            output.writeUTF(metadata.getColumnName(i));
            output.writeUTF(metadata.getColumnTypeName(i));
            output.writeInt(metadata.getColumnType(i));
            output.writeInt(metadata.getColumnDisplaySize(i));
            output.writeInt(metadata.getPrecision(i));
            output.writeUTF(metadata.getTableName(i));
            output.writeInt(metadata.getScale(i));
            output.writeUTF(metadata.getSchemaName(i));
        }

    }

    static class ColumnDef {

        final String catalog;
        final String className;
        final String label;
        final String name;
        final String typeName;
        final int type;
        final int displaySize;
        final int precision;
        final String tableName;
        final String schemaName;

        ColumnDef(final ObjectInputStream input) throws IOException {
            catalog = input.readUTF();
            className = input.readUTF();
            label = input.readUTF();
            name = input.readUTF();
            typeName = input.readUTF();
            type = input.readInt();
            displaySize = input.readInt();
            precision = input.readInt();
            tableName = input.readUTF();
            schemaName = input.readUTF();
        }
    }

}
