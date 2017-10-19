/*
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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.zip.GZIPOutputStream;

class ResultSetWriter {

    static void write(final Path resultSetPath, final ResultSet resultSet) throws SQLException {
        try (final FileOutputStream fos = new FileOutputStream(resultSetPath.toFile())) {
            try (final GZIPOutputStream zos = new GZIPOutputStream(fos)) {
                try (final DataOutputStream output = new DataOutputStream(zos)) {
                    writeMetadata(output, resultSet.getMetaData());
                    writeResultSet(output, resultSet);
                }
            }
        } catch (IOException e) {
            throw new SQLException("Error while writing the ResultSet cache file: " + resultSetPath, e);
        }
    }

    static ByteArrayOutputStream write(final ResultSet resultSet) throws SQLException {
        try (final ByteArrayOutputStream fos = new ByteArrayOutputStream()) {
            try (final DataOutputStream output = new DataOutputStream(fos)) {
                writeMetadata(output, resultSet.getMetaData());
                writeResultSet(output, resultSet);
                return fos;
            }
        } catch (IOException e) {
            throw new SQLException("Error while writing the ResultSet cache", e);
        }
    }

    private static void writeMetadata(final DataOutputStream output, final ResultSetMetaData metadata)
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
            output.writeBoolean(metadata.isAutoIncrement(i));
            output.writeBoolean(metadata.isCaseSensitive(i));
            output.writeBoolean(metadata.isCurrency(i));
            output.writeBoolean(metadata.isDefinitelyWritable(i));
            output.writeInt(metadata.isNullable(i));
            output.writeBoolean(metadata.isReadOnly(i));
            output.writeBoolean(metadata.isSearchable(i));
            output.writeBoolean(metadata.isSigned(i));
            output.writeBoolean(metadata.isWritable(i));
        }

    }

    static ColumnDef[] readColumns(final DataInputStream input) throws IOException {
        final int columnCount = input.readInt();
        final ColumnDef[] columns = new ColumnDef[columnCount];
        for (int i = 0; i < columnCount; i++)
            columns[i] = new ColumnDef(input);
        return columns;
    }

    final static class ColumnDef {

        final String catalog;
        final String className;
        final String label;
        final String name;
        final String typeName;
        final int type;
        final int displaySize;
        final int precision;
        final String tableName;
        final int scale;
        final String schemaName;
        final boolean isAutoIncrement;
        final boolean isCaseSensitive;
        final boolean isCurrency;
        final boolean isDefinitelyWritable;
        final int isNullable;
        final boolean isReadOnly;
        final boolean isSearchable;
        final boolean isSigned;
        final boolean isWritable;

        private ColumnDef(final DataInputStream input) throws IOException {
            catalog = input.readUTF();
            className = input.readUTF();
            label = input.readUTF();
            name = input.readUTF();
            typeName = input.readUTF();
            type = input.readInt();
            displaySize = input.readInt();
            precision = input.readInt();
            tableName = input.readUTF();
            scale = input.readInt();
            schemaName = input.readUTF();
            isAutoIncrement = input.readBoolean();
            isCaseSensitive = input.readBoolean();
            isCurrency = input.readBoolean();
            isDefinitelyWritable = input.readBoolean();
            isNullable = input.readInt();
            isReadOnly = input.readBoolean();
            isSearchable = input.readBoolean();
            isSigned = input.readBoolean();
            isWritable = input.readBoolean();
        }
    }

    private static void writeResultSet(final DataOutputStream output, final ResultSet resultSet)
            throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int[] types = new int[metaData.getColumnCount()];
        for (int i = 0; i < types.length; i++)
            types[i] = metaData.getColumnType(i + 1);
        int pos = 0;
        while (resultSet.next()) {
            output.writeInt(++pos);
            int i = 0;
            for (int type : types) {
                i++;
                switch (type) {
                case Types.BIT:
                case Types.BOOLEAN:
                    writeBoolean(i, resultSet, output);
                    break;
                case Types.TINYINT:
                    writeByte(i, resultSet, output);
                    break;
                case Types.SMALLINT:
                    writeShort(i, resultSet, output);
                    break;
                case Types.INTEGER:
                    writeInteger(i, resultSet, output);
                    break;
                case Types.BIGINT:
                    writeLong(i, resultSet, output);
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    writeFloat(i, resultSet, output);
                    break;
                case Types.DOUBLE:
                    writeDouble(i, resultSet, output);
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    writeBigDecimal(i, resultSet, output);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    writeString(i, resultSet, output);
                    break;
                case Types.DATE:
                    writeDate(i, resultSet, output);
                    break;
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    writeTime(i, resultSet, output);
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    writeTimestamp(i, resultSet, output);
                    break;
                case Types.ROWID:
                    writeRowId(i, resultSet, output);
                    break;
                case Types.CLOB:
                    writeClob(i, resultSet, output);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.NULL:
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.DISTINCT:
                case Types.STRUCT:
                case Types.ARRAY:
                case Types.BLOB:
                case Types.REF:
                case Types.DATALINK:
                case Types.NCLOB:
                case Types.SQLXML:
                case Types.REF_CURSOR:
                    writeNull(output);
                    break;
                }
            }
        }
    }

    private static void writeBoolean(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final boolean val = resultSet.getBoolean(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeBoolean(val);
    }

    private static void writeByte(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final byte val = resultSet.getByte(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeByte(val);
    }

    private static void writeShort(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final short val = resultSet.getShort(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeShort(val);
    }

    private static void writeInteger(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final int val = resultSet.getInt(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeInt(val);
    }

    private static void writeLong(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final long val = resultSet.getLong(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeLong(val);
    }

    private static void writeFloat(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final float val = resultSet.getFloat(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeFloat(val);
    }

    private static void writeDouble(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final double val = resultSet.getDouble(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeDouble(val);
    }

    private static void writeString(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final String val = resultSet.getString(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeUTF(val);
    }

    private static void writeBigDecimal(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final BigDecimal val = resultSet.getBigDecimal(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (wasNull)
            return;
        output.writeDouble(val.doubleValue());
    }

    private static void writeDate(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final Date val = resultSet.getDate(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeLong(val.getTime());
    }

    private static void writeTime(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final Time val = resultSet.getTime(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeLong(val.getTime());
    }

    private static void writeTimestamp(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final Timestamp val = resultSet.getTimestamp(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeLong(val.getTime());
    }

    private static void writeRowId(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final RowId val = resultSet.getRowId(column);
        final boolean wasNull = resultSet.wasNull();
        output.writeBoolean(!wasNull);
        if (!wasNull)
            output.writeUTF(val.toString());
    }

    private static void writeClob(final int column, final ResultSet resultSet, final DataOutputStream output)
            throws SQLException, IOException {
        final Clob clob = resultSet.getClob(column);
        try {
            final boolean wasNull = resultSet.wasNull();
            output.writeBoolean(!wasNull);
            if (wasNull)
                return;
            // TODO Support reader for long sized string
            final long size = clob.length();
            output.writeUTF(size == 0 ? "" : clob.getSubString(1, (int) clob.length()));
        } finally {
            if (clob != null) {
                try {
                    clob.free();
                } catch (AbstractMethodError e) {
                    // May occur with old JDBC drivers
                }
            }
        }
    }

    private static void writeNull(final DataOutputStream output) throws IOException {
        output.writeBoolean(false);
    }

    public static Object readRow(final int type, final DataInputStream input) throws IOException {
        final boolean wasNull = !input.readBoolean();
        if (wasNull)
            return null;
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
            return input.readBoolean();
        case Types.TINYINT:
            return input.readByte();
        case Types.SMALLINT:
            return input.readShort();
        case Types.INTEGER:
            return input.readInt();
        case Types.BIGINT:
            return input.readLong();
        case Types.FLOAT:
        case Types.REAL:
            return input.readFloat();
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
            return input.readDouble();
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            return input.readUTF();
        case Types.DATE:
            return new java.sql.Date(input.readLong());
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE:
            return new java.sql.Time(input.readLong());
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
            return new java.sql.Timestamp(input.readLong());
        case Types.ROWID:
            return input.readUTF();
        case Types.CLOB:
            return input.readUTF();
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.NULL:
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.DISTINCT:
        case Types.STRUCT:
        case Types.ARRAY:
        case Types.BLOB:
        case Types.REF:
        case Types.DATALINK:
        case Types.NCLOB:
        case Types.SQLXML:
        case Types.REF_CURSOR:
        default:
            throw new IOException("Column type no supported: " + type);
        }
    }
}
