namespace Glacier.Formats.Parquet;

using System;
using System.Buffers;
using System.Collections.Generic;

/// <summary>
/// Pure C# Thrift serializer and deserializer for Parquet <see cref="PageHeader"/> and <see cref="FileMetaData"/> structures.
/// </summary>
public static class ParquetThriftCodec
{
    /// <summary>
    /// Writes a Parquet <see cref="PageHeader"/> to a <see cref="ThriftCompactWriter"/>.
    /// </summary>
    /// <param name="writer">The Thrift Compact Protocol writer.</param>
    /// <param name="header">The page header to serialize.</param>
    public static void WritePageHeader(ThriftCompactWriter writer, PageHeader header)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(header);

        writer.WriteStructBegin();

        writer.WriteFieldBegin(1, ThriftType.I32);
        writer.WriteI32((int)header.Type);

        writer.WriteFieldBegin(2, ThriftType.I32);
        writer.WriteI32(header.UncompressedPageSize);

        writer.WriteFieldBegin(3, ThriftType.I32);
        writer.WriteI32(header.CompressedPageSize);

        if (header.DataPageHeader != null)
        {
            writer.WriteFieldBegin(5, ThriftType.Struct);
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.I32);
            writer.WriteI32(header.DataPageHeader.NumValues);

            writer.WriteFieldBegin(2, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.Encoding);

            writer.WriteFieldBegin(3, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.DefinitionLevelEncoding);

            writer.WriteFieldBegin(4, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.RepetitionLevelEncoding);

            writer.WriteStructEnd();
        }
        else if (header.DictionaryPageHeader != null)
        {
            writer.WriteFieldBegin(7, ThriftType.Struct);
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.I32);
            writer.WriteI32(header.DictionaryPageHeader.NumValues);

            writer.WriteFieldBegin(2, ThriftType.I32);
            writer.WriteI32((int)header.DictionaryPageHeader.Encoding);

            writer.WriteStructEnd();
        }

        writer.WriteStructEnd();
    }

    internal static void WritePageHeader(IBufferWriter<byte> bufferWriter, PageHeader header)
    {
        var writer = new ThriftBufferWriter(bufferWriter);
        writer.WriteStructBegin();

        writer.WriteFieldBegin(1, ThriftType.I32);
        writer.WriteI32((int)header.Type);

        writer.WriteFieldBegin(2, ThriftType.I32);
        writer.WriteI32(header.UncompressedPageSize);

        writer.WriteFieldBegin(3, ThriftType.I32);
        writer.WriteI32(header.CompressedPageSize);

        if (header.DataPageHeader != null)
        {
            writer.WriteFieldBegin(5, ThriftType.Struct);
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.I32);
            writer.WriteI32(header.DataPageHeader.NumValues);

            writer.WriteFieldBegin(2, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.Encoding);

            writer.WriteFieldBegin(3, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.DefinitionLevelEncoding);

            writer.WriteFieldBegin(4, ThriftType.I32);
            writer.WriteI32((int)header.DataPageHeader.RepetitionLevelEncoding);

            writer.WriteStructEnd();
        }
        else if (header.DictionaryPageHeader != null)
        {
            writer.WriteFieldBegin(7, ThriftType.Struct);
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.I32);
            writer.WriteI32(header.DictionaryPageHeader.NumValues);

            writer.WriteFieldBegin(2, ThriftType.I32);
            writer.WriteI32((int)header.DictionaryPageHeader.Encoding);

            writer.WriteStructEnd();
        }

        writer.WriteStructEnd();
    }

    /// <summary>
    /// Reads a Parquet <see cref="PageHeader"/> from a <see cref="ThriftCompactReader"/>.
    /// </summary>
    /// <param name="reader">The Thrift Compact Protocol reader.</param>
    /// <returns>The deserialized <see cref="PageHeader"/>.</returns>
    public static PageHeader ReadPageHeader(ThriftCompactReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var header = new PageHeader();
        reader.ReadStructBegin();

        while (reader.ReadFieldBegin(out short fieldId, out var type))
        {
            switch (fieldId)
            {
                case 1:
                    header.Type = (PageType)reader.ReadI32();
                    break;
                case 2:
                    header.UncompressedPageSize = reader.ReadI32();
                    break;
                case 3:
                    header.CompressedPageSize = reader.ReadI32();
                    break;
                case 5: // DataPageHeader
                    header.DataPageHeader = ReadDataPageHeader(reader);
                    break;
                case 7: // DictionaryPageHeader
                    header.DictionaryPageHeader = ReadDictionaryPageHeader(reader);
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        return header;
    }

    private static DataPageHeader ReadDataPageHeader(ThriftCompactReader reader)
    {
        var dph = new DataPageHeader();
        reader.ReadStructBegin();
        while (reader.ReadFieldBegin(out short fieldId, out var type))
        {
            switch (fieldId)
            {
                case 1:
                    dph.NumValues = reader.ReadI32();
                    break;
                case 2:
                    dph.Encoding = (ParquetEncoding)reader.ReadI32();
                    break;
                case 3:
                    dph.DefinitionLevelEncoding = (ParquetEncoding)reader.ReadI32();
                    break;
                case 4:
                    dph.RepetitionLevelEncoding = (ParquetEncoding)reader.ReadI32();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }
        return dph;
    }

    private static DictionaryPageHeader ReadDictionaryPageHeader(ThriftCompactReader reader)
    {
        var dph = new DictionaryPageHeader();
        reader.ReadStructBegin();
        while (reader.ReadFieldBegin(out short fieldId, out var type))
        {
            switch (fieldId)
            {
                case 1:
                    dph.NumValues = reader.ReadI32();
                    break;
                case 2:
                    dph.Encoding = (ParquetEncoding)reader.ReadI32();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }
        return dph;
    }

    /// <summary>
    /// Writes a Parquet <see cref="FileMetaData"/> footer to a <see cref="ThriftCompactWriter"/>.
    /// </summary>
    /// <param name="writer">The Thrift Compact Protocol writer.</param>
    /// <param name="meta">The file metadata to serialize.</param>
    public static void WriteFileMetaData(ThriftCompactWriter writer, FileMetaData meta)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(meta);

        writer.WriteStructBegin();

        writer.WriteFieldBegin(1, ThriftType.I32);
        writer.WriteI32(meta.Version);

        writer.WriteFieldBegin(2, ThriftType.List);
        writer.WriteListBegin(ThriftType.Struct, meta.Schema.Count);
        foreach (var elem in meta.Schema)
        {
            writer.WriteStructBegin();
            if (elem.Type.HasValue)
            {
                writer.WriteFieldBegin(1, ThriftType.I32);
                writer.WriteI32((int)elem.Type.Value);
            }
            if (elem.RepetitionType.HasValue)
            {
                writer.WriteFieldBegin(3, ThriftType.I32);
                writer.WriteI32((int)elem.RepetitionType.Value);
            }
            writer.WriteFieldBegin(4, ThriftType.Binary);
            writer.WriteString(elem.Name);
            if (elem.NumChildren.HasValue)
            {
                writer.WriteFieldBegin(5, ThriftType.I32);
                writer.WriteI32(elem.NumChildren.Value);
            }
            if (elem.ConvertedType.HasValue)
            {
                writer.WriteFieldBegin(6, ThriftType.I32);
                writer.WriteI32(elem.ConvertedType.Value);
            }
            writer.WriteStructEnd();
        }

        writer.WriteFieldBegin(3, ThriftType.I64);
        writer.WriteI64(meta.NumRows);

        writer.WriteFieldBegin(4, ThriftType.List);
        writer.WriteListBegin(ThriftType.Struct, meta.RowGroups.Count);
        foreach (var rg in meta.RowGroups)
        {
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.List);
            writer.WriteListBegin(ThriftType.Struct, rg.Columns.Count);
            foreach (var col in rg.Columns)
            {
                writer.WriteStructBegin();
                writer.WriteFieldBegin(2, ThriftType.I64);
                writer.WriteI64(col.FileOffset);

                if (col.MetaData != null)
                {
                    writer.WriteFieldBegin(3, ThriftType.Struct);
                    writer.WriteStructBegin();

                    writer.WriteFieldBegin(1, ThriftType.I32);
                    writer.WriteI32((int)col.MetaData.Type);

                    writer.WriteFieldBegin(2, ThriftType.List);
                    writer.WriteListBegin(ThriftType.I32, col.MetaData.Encodings.Count);
                    foreach (var enc in col.MetaData.Encodings) writer.WriteI32((int)enc);

                    writer.WriteFieldBegin(3, ThriftType.List);
                    writer.WriteListBegin(ThriftType.Binary, col.MetaData.PathInSchema.Count);
                    foreach (var p in col.MetaData.PathInSchema) writer.WriteString(p);

                    writer.WriteFieldBegin(4, ThriftType.I32);
                    writer.WriteI32((int)col.MetaData.Codec);

                    writer.WriteFieldBegin(5, ThriftType.I64);
                    writer.WriteI64(col.MetaData.NumValues);

                    writer.WriteFieldBegin(6, ThriftType.I64);
                    writer.WriteI64(col.MetaData.TotalUncompressedSize);

                    writer.WriteFieldBegin(7, ThriftType.I64);
                    writer.WriteI64(col.MetaData.TotalCompressedSize);

                    writer.WriteFieldBegin(9, ThriftType.I64);
                    writer.WriteI64(col.MetaData.DataPageOffset);

                    if (col.MetaData.DictionaryPageOffset.HasValue)
                    {
                        writer.WriteFieldBegin(11, ThriftType.I64);
                        writer.WriteI64(col.MetaData.DictionaryPageOffset.Value);
                    }

                    writer.WriteStructEnd();
                }

                writer.WriteStructEnd();
            }

            writer.WriteFieldBegin(2, ThriftType.I64);
            writer.WriteI64(rg.TotalByteSize);

            writer.WriteFieldBegin(3, ThriftType.I64);
            writer.WriteI64(rg.NumRows);

            writer.WriteStructEnd();
        }

        if (meta.CreatedBy != null)
        {
            writer.WriteFieldBegin(6, ThriftType.Binary);
            writer.WriteString(meta.CreatedBy);
        }

        writer.WriteStructEnd();
    }

    internal static void WriteFileMetaData(IBufferWriter<byte> bufferWriter, FileMetaData meta)
    {
        var writer = new ThriftBufferWriter(bufferWriter);
        writer.WriteStructBegin();

        writer.WriteFieldBegin(1, ThriftType.I32);
        writer.WriteI32(meta.Version);

        writer.WriteFieldBegin(2, ThriftType.List);
        writer.WriteListBegin(ThriftType.Struct, meta.Schema.Count);
        foreach (var elem in meta.Schema)
        {
            writer.WriteStructBegin();
            if (elem.Type.HasValue)
            {
                writer.WriteFieldBegin(1, ThriftType.I32);
                writer.WriteI32((int)elem.Type.Value);
            }
            if (elem.RepetitionType.HasValue)
            {
                writer.WriteFieldBegin(3, ThriftType.I32);
                writer.WriteI32((int)elem.RepetitionType.Value);
            }
            writer.WriteFieldBegin(4, ThriftType.Binary);
            writer.WriteString(elem.Name);
            if (elem.NumChildren.HasValue)
            {
                writer.WriteFieldBegin(5, ThriftType.I32);
                writer.WriteI32(elem.NumChildren.Value);
            }
            if (elem.ConvertedType.HasValue)
            {
                writer.WriteFieldBegin(6, ThriftType.I32);
                writer.WriteI32(elem.ConvertedType.Value);
            }
            writer.WriteStructEnd();
        }

        writer.WriteFieldBegin(3, ThriftType.I64);
        writer.WriteI64(meta.NumRows);

        writer.WriteFieldBegin(4, ThriftType.List);
        writer.WriteListBegin(ThriftType.Struct, meta.RowGroups.Count);
        foreach (var rg in meta.RowGroups)
        {
            writer.WriteStructBegin();

            writer.WriteFieldBegin(1, ThriftType.List);
            writer.WriteListBegin(ThriftType.Struct, rg.Columns.Count);
            foreach (var col in rg.Columns)
            {
                writer.WriteStructBegin();
                writer.WriteFieldBegin(2, ThriftType.I64);
                writer.WriteI64(col.FileOffset);

                if (col.MetaData != null)
                {
                    writer.WriteFieldBegin(3, ThriftType.Struct);
                    writer.WriteStructBegin();

                    writer.WriteFieldBegin(1, ThriftType.I32);
                    writer.WriteI32((int)col.MetaData.Type);

                    writer.WriteFieldBegin(2, ThriftType.List);
                    writer.WriteListBegin(ThriftType.I32, col.MetaData.Encodings.Count);
                    foreach (var enc in col.MetaData.Encodings) writer.WriteI32((int)enc);

                    writer.WriteFieldBegin(3, ThriftType.List);
                    writer.WriteListBegin(ThriftType.Binary, col.MetaData.PathInSchema.Count);
                    foreach (var p in col.MetaData.PathInSchema) writer.WriteString(p);

                    writer.WriteFieldBegin(4, ThriftType.I32);
                    writer.WriteI32((int)col.MetaData.Codec);

                    writer.WriteFieldBegin(5, ThriftType.I64);
                    writer.WriteI64(col.MetaData.NumValues);

                    writer.WriteFieldBegin(6, ThriftType.I64);
                    writer.WriteI64(col.MetaData.TotalUncompressedSize);

                    writer.WriteFieldBegin(7, ThriftType.I64);
                    writer.WriteI64(col.MetaData.TotalCompressedSize);

                    writer.WriteFieldBegin(9, ThriftType.I64);
                    writer.WriteI64(col.MetaData.DataPageOffset);

                    if (col.MetaData.DictionaryPageOffset.HasValue)
                    {
                        writer.WriteFieldBegin(11, ThriftType.I64);
                        writer.WriteI64(col.MetaData.DictionaryPageOffset.Value);
                    }

                    writer.WriteStructEnd();
                }

                writer.WriteStructEnd();
            }

            writer.WriteFieldBegin(2, ThriftType.I64);
            writer.WriteI64(rg.TotalByteSize);

            writer.WriteFieldBegin(3, ThriftType.I64);
            writer.WriteI64(rg.NumRows);

            writer.WriteStructEnd();
        }

        if (meta.CreatedBy != null)
        {
            writer.WriteFieldBegin(6, ThriftType.Binary);
            writer.WriteString(meta.CreatedBy);
        }

        writer.WriteStructEnd();
    }

    /// <summary>
    /// Reads a Parquet <see cref="FileMetaData"/> footer from a <see cref="ThriftCompactReader"/>.
    /// </summary>
    /// <param name="reader">The Thrift Compact Protocol reader.</param>
    /// <returns>The deserialized <see cref="FileMetaData"/>.</returns>
    public static FileMetaData ReadFileMetaData(ThriftCompactReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var meta = new FileMetaData();
        reader.ReadStructBegin();

        while (reader.ReadFieldBegin(out short fieldId, out var type))
        {
            switch (fieldId)
            {
                case 1:
                    meta.Version = reader.ReadI32();
                    break;
                case 2:
                    meta.Schema = ReadSchema(reader);
                    break;
                case 3:
                    meta.NumRows = reader.ReadI64();
                    break;
                case 4:
                    meta.RowGroups = ReadRowGroups(reader);
                    break;
                case 6:
                    meta.CreatedBy = reader.ReadString();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        return meta;
    }

    internal static FileMetaData ReadFileMetaData(ReadOnlySpan<byte> metaSpan)
    {
        var reader = new ThriftSpanReader(metaSpan);
        var meta = new FileMetaData();
        reader.ReadStructBegin();

        while (reader.ReadFieldBegin(out short fieldId, out var type))
        {
            switch (fieldId)
            {
                case 1:
                    meta.Version = reader.ReadI32();
                    break;
                case 2:
                    meta.Schema = ReadSchema(ref reader);
                    break;
                case 3:
                    meta.NumRows = reader.ReadI64();
                    break;
                case 4:
                    meta.RowGroups = ReadRowGroups(ref reader);
                    break;
                case 6:
                    meta.CreatedBy = reader.ReadString();
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        return meta;
    }

    private static List<SchemaElement> ReadSchema(ThriftCompactReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var list = new List<SchemaElement>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var elem = new SchemaElement();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 1:
                        elem.Type = (ParquetType)reader.ReadI32();
                        break;
                    case 3:
                        elem.RepetitionType = (FieldRepetitionType)reader.ReadI32();
                        break;
                    case 4:
                        elem.Name = reader.ReadString();
                        break;
                    case 5:
                        elem.NumChildren = reader.ReadI32();
                        break;
                    case 6:
                        elem.ConvertedType = reader.ReadI32();
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            list.Add(elem);
        }
        return list;
    }

    private static List<SchemaElement> ReadSchema(ref ThriftSpanReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var list = new List<SchemaElement>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var elem = new SchemaElement();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 1:
                        elem.Type = (ParquetType)reader.ReadI32();
                        break;
                    case 3:
                        elem.RepetitionType = (FieldRepetitionType)reader.ReadI32();
                        break;
                    case 4:
                        elem.Name = reader.ReadString();
                        break;
                    case 5:
                        elem.NumChildren = reader.ReadI32();
                        break;
                    case 6:
                        elem.ConvertedType = reader.ReadI32();
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            list.Add(elem);
        }
        return list;
    }

    private static List<RowGroup> ReadRowGroups(ThriftCompactReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var rowGroups = new List<RowGroup>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var rg = new RowGroup();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 1:
                        rg.Columns = ReadColumns(reader);
                        break;
                    case 2:
                        rg.TotalByteSize = reader.ReadI64();
                        break;
                    case 3:
                        rg.NumRows = reader.ReadI64();
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            rowGroups.Add(rg);
        }
        return rowGroups;
    }

    private static List<RowGroup> ReadRowGroups(ref ThriftSpanReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var rowGroups = new List<RowGroup>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var rg = new RowGroup();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 1:
                        rg.Columns = ReadColumns(ref reader);
                        break;
                    case 2:
                        rg.TotalByteSize = reader.ReadI64();
                        break;
                    case 3:
                        rg.NumRows = reader.ReadI64();
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            rowGroups.Add(rg);
        }
        return rowGroups;
    }

    private static List<ColumnChunk> ReadColumns(ThriftCompactReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var cols = new List<ColumnChunk>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var chunk = new ColumnChunk();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 2:
                        chunk.FileOffset = reader.ReadI64();
                        break;
                    case 3:
                        chunk.MetaData = ReadColumnMetaData(reader);
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            cols.Add(chunk);
        }
        return cols;
    }

    private static List<ColumnChunk> ReadColumns(ref ThriftSpanReader reader)
    {
        reader.ReadListBegin(out _, out int size);
        var cols = new List<ColumnChunk>(Math.Min(size, 4096));
        for (int i = 0; i < size; i++)
        {
            var chunk = new ColumnChunk();
            reader.ReadStructBegin();
            while (reader.ReadFieldBegin(out short fId, out var fType))
            {
                switch (fId)
                {
                    case 2:
                        chunk.FileOffset = reader.ReadI64();
                        break;
                    case 3:
                        chunk.MetaData = ReadColumnMetaData(ref reader);
                        break;
                    default:
                        reader.Skip(fType);
                        break;
                }
            }
            cols.Add(chunk);
        }
        return cols;
    }

    private static ColumnMetaData ReadColumnMetaData(ThriftCompactReader reader)
    {
        var meta = new ColumnMetaData();
        reader.ReadStructBegin();
        while (reader.ReadFieldBegin(out short fId, out var fType))
        {
            switch (fId)
            {
                case 1:
                    meta.Type = (ParquetType)reader.ReadI32();
                    break;
                case 2:
                    reader.ReadListBegin(out _, out int encCount);
                    for (int j = 0; j < encCount; j++) meta.Encodings.Add((ParquetEncoding)reader.ReadI32());
                    break;
                case 3:
                    reader.ReadListBegin(out _, out int pathCount);
                    for (int j = 0; j < pathCount; j++) meta.PathInSchema.Add(reader.ReadString());
                    break;
                case 4:
                    meta.Codec = (CompressionCodec)reader.ReadI32();
                    break;
                case 5:
                    meta.NumValues = reader.ReadI64();
                    break;
                case 6:
                    meta.TotalUncompressedSize = reader.ReadI64();
                    break;
                case 7:
                    meta.TotalCompressedSize = reader.ReadI64();
                    break;
                case 9:
                    meta.DataPageOffset = reader.ReadI64();
                    break;
                case 11:
                    meta.DictionaryPageOffset = reader.ReadI64();
                    break;
                default:
                    reader.Skip(fType);
                    break;
            }
        }
        return meta;
    }

    private static ColumnMetaData ReadColumnMetaData(ref ThriftSpanReader reader)
    {
        var meta = new ColumnMetaData();
        reader.ReadStructBegin();
        while (reader.ReadFieldBegin(out short fId, out var fType))
        {
            switch (fId)
            {
                case 1:
                    meta.Type = (ParquetType)reader.ReadI32();
                    break;
                case 2:
                    reader.ReadListBegin(out _, out int encCount);
                    for (int j = 0; j < encCount; j++) meta.Encodings.Add((ParquetEncoding)reader.ReadI32());
                    break;
                case 3:
                    reader.ReadListBegin(out _, out int pathCount);
                    for (int j = 0; j < pathCount; j++) meta.PathInSchema.Add(reader.ReadString());
                    break;
                case 4:
                    meta.Codec = (CompressionCodec)reader.ReadI32();
                    break;
                case 5:
                    meta.NumValues = reader.ReadI64();
                    break;
                case 6:
                    meta.TotalUncompressedSize = reader.ReadI64();
                    break;
                case 7:
                    meta.TotalCompressedSize = reader.ReadI64();
                    break;
                case 9:
                    meta.DataPageOffset = reader.ReadI64();
                    break;
                case 11:
                    meta.DictionaryPageOffset = reader.ReadI64();
                    break;
                default:
                    reader.Skip(fType);
                    break;
            }
        }
        return meta;
    }
}
