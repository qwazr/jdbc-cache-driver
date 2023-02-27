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
 */package io.github.jhstatewide.jdbc.cache

import java.io.*
import java.nio.file.Path
import java.sql.*
import java.util.zip.GZIPOutputStream

internal object ResultSetWriter {
    @Throws(SQLException::class)
    fun write(resultSetPath: Path, resultSet: ResultSet) {
        try {
            FileOutputStream(resultSetPath.toFile()).use { fos ->
                GZIPOutputStream(fos).use { zos ->
                    DataOutputStream(zos).use { output ->
                        writeMetadata(output, resultSet.metaData)
                        writeResultSet(output, resultSet)
                    }
                }
            }
        } catch (e: IOException) {
            throw SQLException("Error while writing the ResultSet cache file: $resultSetPath", e)
        }
    }

    @Throws(SQLException::class)
    fun write(resultSet: ResultSet): ByteArrayOutputStream {
        try {
            ByteArrayOutputStream().use { fos ->
                DataOutputStream(fos).use { output ->
                    writeMetadata(output, resultSet.metaData)
                    writeResultSet(output, resultSet)
                    return fos
                }
            }
        } catch (e: IOException) {
            throw SQLException("Error while writing the ResultSet cache", e)
        }
    }

    @Throws(IOException::class, SQLException::class)
    private fun writeMetadata(output: DataOutputStream, metadata: ResultSetMetaData) {
        val columnCount = metadata.columnCount
        output.writeInt(columnCount)
        for (i in 1..columnCount) {
            output.writeUTF(metadata.getCatalogName(i))
            output.writeUTF(metadata.getColumnClassName(i))
            output.writeUTF(metadata.getColumnLabel(i))
            output.writeUTF(metadata.getColumnName(i))
            output.writeUTF(metadata.getColumnTypeName(i))
            output.writeInt(metadata.getColumnType(i))
            output.writeInt(metadata.getColumnDisplaySize(i))
            output.writeInt(metadata.getPrecision(i))
            output.writeUTF(metadata.getTableName(i))
            output.writeInt(metadata.getScale(i))
            output.writeUTF(metadata.getSchemaName(i))
            output.writeBoolean(metadata.isAutoIncrement(i))
            output.writeBoolean(metadata.isCaseSensitive(i))
            output.writeBoolean(metadata.isCurrency(i))
            output.writeBoolean(metadata.isDefinitelyWritable(i))
            output.writeInt(metadata.isNullable(i))
            output.writeBoolean(metadata.isReadOnly(i))
            output.writeBoolean(metadata.isSearchable(i))
            output.writeBoolean(metadata.isSigned(i))
            output.writeBoolean(metadata.isWritable(i))
        }
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readColumns(input: DataInputStream): Array<ColumnDef> {
        val columnCount = input.readInt()
        return Array(columnCount) { ColumnDef(input) }
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeResultSet(output: DataOutputStream, resultSet: ResultSet) {
        val metaData = resultSet.metaData
        val types = IntArray(metaData.columnCount)
        for (i in types.indices) types[i] = metaData.getColumnType(i + 1)
        var pos = 0
        while (resultSet.next()) {
            output.writeInt(++pos)
            var i = 0
            for (type in types) {
                i++
                when (type) {
                    Types.BIT, Types.BOOLEAN -> writeBoolean(i, resultSet, output)
                    Types.TINYINT -> writeByte(i, resultSet, output)
                    Types.SMALLINT -> writeShort(i, resultSet, output)
                    Types.INTEGER -> writeInteger(i, resultSet, output)
                    Types.BIGINT -> writeLong(i, resultSet, output)
                    Types.FLOAT, Types.REAL -> writeFloat(i, resultSet, output)
                    Types.DOUBLE -> writeDouble(i, resultSet, output)
                    Types.NUMERIC, Types.DECIMAL -> writeBigDecimal(i, resultSet, output)
                    Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> writeString(
                        i,
                        resultSet,
                        output
                    )

                    Types.DATE -> writeDate(i, resultSet, output)
                    Types.TIME, Types.TIME_WITH_TIMEZONE -> writeTime(i, resultSet, output)
                    Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> writeTimestamp(i, resultSet, output)
                    Types.ROWID -> writeRowId(i, resultSet, output)
                    Types.CLOB -> writeClob(i, resultSet, output)
                    Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.NULL, Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.STRUCT, Types.ARRAY, Types.BLOB, Types.REF, Types.DATALINK, Types.NCLOB, Types.SQLXML, Types.REF_CURSOR -> writeNull(
                        output
                    )
                }
            }
        }
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeBoolean(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getBoolean(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeBoolean(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeByte(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getByte(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeByte(`val`.toInt())
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeShort(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getShort(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeShort(`val`.toInt())
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeInteger(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getInt(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeInt(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeLong(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getLong(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeLong(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeFloat(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getFloat(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeFloat(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeDouble(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getDouble(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeDouble(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeString(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getString(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeUTF(`val`)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeBigDecimal(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getBigDecimal(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (wasNull) return
        output.writeDouble(`val`.toDouble())
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeDate(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getDate(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeLong(`val`.time)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeTime(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getTime(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeLong(`val`.time)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeTimestamp(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getTimestamp(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeLong(`val`.time)
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeRowId(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val `val` = resultSet.getRowId(column)
        val wasNull = resultSet.wasNull()
        output.writeBoolean(!wasNull)
        if (!wasNull) output.writeUTF(`val`.toString())
    }

    @Throws(SQLException::class, IOException::class)
    private fun writeClob(column: Int, resultSet: ResultSet, output: DataOutputStream) {
        val clob = resultSet.getClob(column)
        try {
            val wasNull = resultSet.wasNull()
            output.writeBoolean(!wasNull)
            if (wasNull) return
            // TODO Support reader for long sized string
            val size = clob!!.length()
            output.writeUTF(if (size == 0L) "" else clob.getSubString(1, clob.length().toInt()))
        } finally {
            if (clob != null) {
                try {
                    clob.free()
                } catch (e: AbstractMethodError) {
                    // May occur with old JDBC drivers
                }
            }
        }
    }

    @Throws(IOException::class)
    private fun writeNull(output: DataOutputStream) {
        output.writeBoolean(false)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readRow(type: Int, input: DataInputStream): Any? {
        val wasNull = !input.readBoolean()
        return if (wasNull) null else when (type) {
            Types.BIT, Types.BOOLEAN -> input.readBoolean()
            Types.TINYINT -> input.readByte()
            Types.SMALLINT -> input.readShort()
            Types.INTEGER -> input.readInt()
            Types.BIGINT -> input.readLong()
            Types.FLOAT, Types.REAL -> input.readFloat()
            Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> input.readDouble()
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> input.readUTF()
            Types.DATE -> Date(input.readLong())
            Types.TIME, Types.TIME_WITH_TIMEZONE -> Time(input.readLong())
            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Timestamp(input.readLong())
            Types.ROWID -> input.readUTF()
            Types.CLOB -> input.readUTF()
            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.NULL, Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.STRUCT, Types.ARRAY, Types.BLOB, Types.REF, Types.DATALINK, Types.NCLOB, Types.SQLXML, Types.REF_CURSOR -> throw IOException(
                "Column type no supported: $type"
            )

            else -> throw IOException("Column type no supported: $type")
        }
    }

    internal class ColumnDef constructor(input: DataInputStream) {
        val catalog: String
        val className: String
        @JvmField
        val label: String
        val name: String
        val typeName: String
        @JvmField
        val type: Int
        val displaySize: Int
        val precision: Int
        val tableName: String
        val scale: Int
        val schemaName: String
        val isAutoIncrement: Boolean
        val isCaseSensitive: Boolean
        val isCurrency: Boolean
        val isDefinitelyWritable: Boolean
        val isNullable: Int
        val isReadOnly: Boolean
        val isSearchable: Boolean
        val isSigned: Boolean
        val isWritable: Boolean

        init {
            catalog = input.readUTF()
            className = input.readUTF()
            label = input.readUTF()
            name = input.readUTF()
            typeName = input.readUTF()
            type = input.readInt()
            displaySize = input.readInt()
            precision = input.readInt()
            tableName = input.readUTF()
            scale = input.readInt()
            schemaName = input.readUTF()
            isAutoIncrement = input.readBoolean()
            isCaseSensitive = input.readBoolean()
            isCurrency = input.readBoolean()
            isDefinitelyWritable = input.readBoolean()
            isNullable = input.readInt()
            isReadOnly = input.readBoolean()
            isSearchable = input.readBoolean()
            isSigned = input.readBoolean()
            isWritable = input.readBoolean()
        }
    }
}