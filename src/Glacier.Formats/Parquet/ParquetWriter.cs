namespace Glacier.Formats.Parquet;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using Glacier.Formats.Arrow;

/// <summary>
/// High-performance Parquet v1 writer supporting <see cref="CompressionCodec.Uncompressed"/> and
/// <see cref="CompressionCodec.Snappy"/> with <c>PLAIN</c> value encoding and RLE definition levels for nullable columns.
/// </summary>
public static class ParquetWriter
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;

    /// <summary>
    /// Writes a single <see cref="ArrowRecordBatch"/> as a Parquet v1 file at <paramref name="filePath"/>.
    /// </summary>
    /// <param name="filePath">The destination file path.</param>
    /// <param name="batch">The record batch to write.</param>
    /// <param name="codec">The page compression codec (<see cref="CompressionCodec.Uncompressed"/> or <see cref="CompressionCodec.Snappy"/>).</param>
    public static void WriteFile(string filePath, ArrowRecordBatch batch, CompressionCodec codec = CompressionCodec.Snappy)
    {
        ArgumentNullException.ThrowIfNull(batch);
        WriteFile(filePath, [batch], codec);
    }

    /// <summary>
    /// Writes multiple <see cref="ArrowRecordBatch"/> instances as Parquet row groups to <paramref name="filePath"/>.
    /// </summary>
    /// <param name="filePath">The destination file path.</param>
    /// <param name="batches">The ordered record batches (one row group per batch).</param>
    /// <param name="codec">The page compression codec (<see cref="CompressionCodec.Uncompressed"/> or <see cref="CompressionCodec.Snappy"/>).</param>
    public static void WriteFile(string filePath, IReadOnlyList<ArrowRecordBatch> batches, CompressionCodec codec = CompressionCodec.Snappy)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None);
        WriteStream(fs, batches, codec);
    }

    /// <summary>
    /// Serializes a single <see cref="ArrowRecordBatch"/> into a Parquet v1 byte array.
    /// </summary>
    /// <param name="batch">The record batch to serialize.</param>
    /// <param name="codec">The page compression codec (<see cref="CompressionCodec.Uncompressed"/> or <see cref="CompressionCodec.Snappy"/>).</param>
    /// <returns>The complete Parquet file bytes.</returns>
    public static byte[] WriteToBytes(ArrowRecordBatch batch, CompressionCodec codec = CompressionCodec.Snappy)
    {
        ArgumentNullException.ThrowIfNull(batch);
        using var ms = new MemoryStream();
        WriteStream(ms, [batch], codec);
        return ms.ToArray();
    }

    /// <summary>
    /// Writes a single <see cref="ArrowRecordBatch"/> as a Parquet v1 stream.
    /// </summary>
    /// <param name="stream">The writable seekable destination stream.</param>
    /// <param name="batch">The record batch to write.</param>
    /// <param name="codec">The page compression codec (<see cref="CompressionCodec.Uncompressed"/> or <see cref="CompressionCodec.Snappy"/>).</param>
    public static void WriteStream(Stream stream, ArrowRecordBatch batch, CompressionCodec codec = CompressionCodec.Snappy)
    {
        ArgumentNullException.ThrowIfNull(batch);
        WriteStream(stream, [batch], codec);
    }

    /// <summary>
    /// Writes multiple <see cref="ArrowRecordBatch"/> instances as Parquet row groups to <paramref name="stream"/>.
    /// </summary>
    /// <param name="stream">The writable seekable destination stream.</param>
    /// <param name="batches">The ordered record batches (one row group per batch).</param>
    /// <param name="codec">The page compression codec (<see cref="CompressionCodec.Uncompressed"/> or <see cref="CompressionCodec.Snappy"/>).</param>
    public static void WriteStream(Stream stream, IReadOnlyList<ArrowRecordBatch> batches, CompressionCodec codec = CompressionCodec.Snappy)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(batches);

        if (codec != CompressionCodec.Uncompressed && codec != CompressionCodec.Snappy)
        {
            throw new NotSupportedException($"Unsupported Parquet compression codec '{codec}'. Supported codecs: Uncompressed, Snappy.");
        }

        // 1. Write PAR1 magic header
        stream.Write(Magic);

        using var bufferWriter = new PooledBufferWriter(4096);

        if (batches.Count == 0)
        {
            var emptyMeta = new FileMetaData
            {
                Version = 1,
                Schema = [new SchemaElement { Name = "schema", NumChildren = 0 }],
                NumRows = 0,
                RowGroups = [],
                CreatedBy = "Glacier.Formats 1.0"
            };
            WriteFooter(stream, emptyMeta, bufferWriter);
            return;
        }

        ArrowRecordBatch firstBatch = batches[0] ?? throw new ArgumentException("Batch at index 0 is null.", nameof(batches));
        var schemaElements = new List<SchemaElement>(firstBatch.Columns.Count + 1)
        {
            new SchemaElement { Name = "schema", NumChildren = firstBatch.Columns.Count }
        };

        var expectedTypes = new ParquetType[firstBatch.Columns.Count];
        var expectedNullable = new bool[firstBatch.Columns.Count];
        for (int i = 0; i < firstBatch.Columns.Count; i++)
        {
            var col = firstBatch.Columns[i];
            ParquetType pType = MapArrowTypeToParquet(col.Field.DataType.Id);
            expectedTypes[i] = pType;
            expectedNullable[i] = col.Field.IsNullable;
            schemaElements.Add(new SchemaElement
            {
                Name = col.Field.Name,
                Type = pType,
                RepetitionType = col.Field.IsNullable ? FieldRepetitionType.Optional : FieldRepetitionType.Required,
                ConvertedType = col.Field.DataType.Id == ArrowTypeId.Utf8 ? 0 : null
            });
        }

        using var headerWriter = new PooledBufferWriter(256);
        var rowGroups = new List<RowGroup>(batches.Count);
        long totalRows = 0;

        for (int rgIdx = 0; rgIdx < batches.Count; rgIdx++)
        {
            var batch = batches[rgIdx] ?? throw new ArgumentException($"Batch at index {rgIdx} is null.", nameof(batches));
            if (batch.RowCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batches), $"Batch {rgIdx} has negative RowCount {batch.RowCount}.");
            }

            if (batch.Columns.Count != firstBatch.Columns.Count)
            {
                throw new ArgumentException($"Batch {rgIdx} column count ({batch.Columns.Count}) does not match schema ({firstBatch.Columns.Count}).", nameof(batches));
            }

            var columnChunks = new List<ColumnChunk>(batch.Columns.Count);
            long totalRowGroupBytes = 0;

            for (int i = 0; i < batch.Columns.Count; i++)
            {
                var col = batch.Columns[i];
                if (col.Length != batch.RowCount)
                {
                    throw new ArgumentException($"Batch {rgIdx} column '{col.Field.Name}' length ({col.Length}) does not match batch RowCount ({batch.RowCount}).", nameof(batches));
                }

                ParquetType pType = MapArrowTypeToParquet(col.Field.DataType.Id);
                if (pType != expectedTypes[i] || col.Field.IsNullable != expectedNullable[i])
                {
                    throw new ArgumentException(
                        $"Batch {rgIdx} column '{col.Field.Name}' schema ({pType}, nullable={col.Field.IsNullable}) does not match initial schema ({expectedTypes[i]}, nullable={expectedNullable[i]}).",
                        nameof(batches));
                }

                if (!col.Field.IsNullable && col.NullCount > 0)
                {
                    throw new ArgumentException(
                        $"Non-nullable column '{col.Field.Name}' contains {col.NullCount} null values.",
                        nameof(batches));
                }

                if (col.NullCount < 0 || col.NullCount > col.Length)
                {
                    throw new ArgumentException(
                        $"Column '{col.Field.Name}' has invalid NullCount={col.NullCount} for Length={col.Length}.",
                        nameof(batches));
                }

                if (col.NullCount > 0 && (col.NullBitmap.IsEmpty || col.NullBitmap.Length < ((col.Length + 7) >> 3)))
                {
                    throw new ArgumentException(
                        $"Column '{col.Field.Name}' has NullCount={col.NullCount} but missing or truncated NullBitmap.",
                        nameof(batches));
                }

                long chunkOffset = stream.Position;

                int uncompressedSize;
                int compressedSize;
                ReadOnlySpan<byte> directUncompressedSpan = ReadOnlySpan<byte>.Empty;
                bool useDirectZeroCopy = false;

                if (!col.Field.IsNullable && codec == CompressionCodec.Uncompressed && TryGetDirectPlainSpan(col, out directUncompressedSpan))
                {
                    useDirectZeroCopy = true;
                    uncompressedSize = directUncompressedSpan.Length;
                    compressedSize = directUncompressedSpan.Length;
                }
                else
                {
                    bufferWriter.Reset();
                    EncodeColumnPage(col, bufferWriter);
                    uncompressedSize = bufferWriter.WrittenCount;
                }

                byte[]? compBuf = null;
                try
                {
                    if (codec == CompressionCodec.Snappy)
                    {
                        int maxComp = SnappyCodec.GetMaxCompressedLength(uncompressedSize);
                        compBuf = ArrayPool<byte>.Shared.Rent(maxComp);
                        compressedSize = SnappyCodec.Compress(bufferWriter.WrittenSpan, compBuf);
                    }
                    else
                    {
                        compressedSize = uncompressedSize;
                    }

                    var pageHeader = new PageHeader
                    {
                        Type = PageType.DataPage,
                        UncompressedPageSize = uncompressedSize,
                        CompressedPageSize = compressedSize,
                        DataPageHeader = new DataPageHeader
                        {
                            NumValues = col.Length,
                            Encoding = ParquetEncoding.Plain,
                            DefinitionLevelEncoding = ParquetEncoding.Rle,
                            RepetitionLevelEncoding = ParquetEncoding.Rle
                        }
                    };

                    headerWriter.Reset();
                    ParquetThriftCodec.WritePageHeader(headerWriter, pageHeader);
                    stream.Write(headerWriter.WrittenSpan);

                    if (compBuf != null)
                    {
                        stream.Write(compBuf.AsSpan(0, compressedSize));
                    }
                    else if (useDirectZeroCopy)
                    {
                        stream.Write(directUncompressedSpan);
                    }
                    else
                    {
                        stream.Write(bufferWriter.WrittenSpan);
                    }

                    long chunkTotalBytes = headerWriter.WrittenCount + (long)compressedSize;
                    totalRowGroupBytes += chunkTotalBytes;

                    List<ParquetEncoding> encodings = col.Field.IsNullable
                        ? [ParquetEncoding.Rle, ParquetEncoding.Plain]
                        : [ParquetEncoding.Plain];

                    var colMeta = new ColumnMetaData
                    {
                        Type = pType,
                        Encodings = encodings,
                        PathInSchema = [col.Field.Name],
                        Codec = codec,
                        NumValues = col.Length,
                        TotalUncompressedSize = headerWriter.WrittenCount + (long)uncompressedSize,
                        TotalCompressedSize = chunkTotalBytes,
                        DataPageOffset = chunkOffset
                    };

                    columnChunks.Add(new ColumnChunk
                    {
                        FileOffset = chunkOffset,
                        MetaData = colMeta
                    });
                }
                finally
                {
                    if (compBuf != null)
                    {
                        ArrayPool<byte>.Shared.Return(compBuf);
                    }
                }
            }

            rowGroups.Add(new RowGroup
            {
                Columns = columnChunks,
                NumRows = batch.RowCount,
                TotalByteSize = totalRowGroupBytes
            });
            totalRows += batch.RowCount;
        }

        var fileMetadata = new FileMetaData
        {
            Version = 1,
            Schema = schemaElements,
            NumRows = totalRows,
            RowGroups = rowGroups,
            CreatedBy = "Glacier.Formats 1.0"
        };

        WriteFooter(stream, fileMetadata, bufferWriter);
    }

    private static void WriteFooter(Stream stream, FileMetaData fileMetadata, PooledBufferWriter bufferWriter)
    {
        bufferWriter.Reset();
        ParquetThriftCodec.WriteFileMetaData(bufferWriter, fileMetadata);
        stream.Write(bufferWriter.WrittenSpan);

        Span<byte> lenBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lenBytes, (uint)bufferWriter.WrittenCount);
        stream.Write(lenBytes);

        stream.Write(Magic);
        stream.Flush();
    }

    private static bool TryGetDirectPlainSpan(ArrowColumn col, out ReadOnlySpan<byte> span)
    {
        switch (col.Field.DataType.Id)
        {
            case ArrowTypeId.Boolean:
            {
                int byteLen = (col.Length + 7) >> 3;
                if (col.DataBuffer.Length < byteLen)
                {
                    throw new ArgumentException($"Boolean column '{col.Field.Name}' DataBuffer is shorter than {byteLen} bytes.");
                }
                span = col.DataBuffer.Span[..byteLen];
                return true;
            }
            case ArrowTypeId.Int32:
            case ArrowTypeId.UInt32:
            case ArrowTypeId.Float:
            case ArrowTypeId.Date32:
            case ArrowTypeId.Int64:
            case ArrowTypeId.UInt64:
            case ArrowTypeId.Double:
            case ArrowTypeId.Date64:
            case ArrowTypeId.Timestamp:
            case ArrowTypeId.Duration:
            case ArrowTypeId.Time64:
            {
                int byteLen = checked(col.Length * col.Field.DataType.ByteWidth);
                if (col.DataBuffer.Length < byteLen)
                {
                    throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer ({col.DataBuffer.Length}) is shorter than {byteLen} bytes.");
                }
                span = col.DataBuffer.Span[..byteLen];
                return true;
            }
            default:
                span = ReadOnlySpan<byte>.Empty;
                return false;
        }
    }

    private static void EncodeColumnPage(ArrowColumn col, PooledBufferWriter writer)
    {
        int rowCount = col.Length;
        int nonNullCount = rowCount;
        uint[]? rentedDefLevels = null;

        try
        {
            Span<uint> defLevels = Span<uint>.Empty;
            if (col.Field.IsNullable)
            {
                if (rowCount == 0)
                {
                    Span<byte> zeroLen = writer.GetSpan(4);
                    BinaryPrimitives.WriteInt32LittleEndian(zeroLen, 0);
                    writer.Advance(4);
                    return;
                }

                rentedDefLevels = ArrayPool<uint>.Shared.Rent(rowCount);
                defLevels = rentedDefLevels.AsSpan(0, rowCount);

                if (col.NullCount == 0 || col.NullBitmap.IsEmpty)
                {
                    defLevels.Fill(1u);
                    nonNullCount = rowCount;
                }
                else
                {
                    nonNullCount = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        bool isNull = col.IsNull(i);
                        defLevels[i] = isNull ? 0u : 1u;
                        if (!isNull) nonNullCount++;
                    }
                }

                int maxRleBytes = ParquetRleCodec.GetMaxEncodedLength(rowCount, 1);
                Span<byte> rleDst = writer.GetSpan(4 + maxRleBytes);
                int rleLen = ParquetRleCodec.Encode(defLevels, 1, rleDst[4..]);
                BinaryPrimitives.WriteInt32LittleEndian(rleDst[..4], rleLen);
                writer.Advance(4 + rleLen);
            }

            if (nonNullCount == 0)
            {
                return;
            }

            bool allValid = nonNullCount == rowCount;

            switch (col.Field.DataType.Id)
            {
                case ArrowTypeId.Utf8:
                case ArrowTypeId.Binary:
                {
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!allValid && defLevels[i] == 0) continue;
                        var bytes = col.GetBytes(i);
                        Span<byte> dst = writer.GetSpan(4 + bytes.Length);
                        BinaryPrimitives.WriteInt32LittleEndian(dst[..4], bytes.Length);
                        bytes.CopyTo(dst[4..]);
                        writer.Advance(4 + bytes.Length);
                    }
                    break;
                }

                case ArrowTypeId.Boolean:
                {
                    int srcByteLen = (rowCount + 7) >> 3;
                    if (col.DataBuffer.Length < srcByteLen)
                    {
                        throw new ArgumentException($"Boolean column '{col.Field.Name}' DataBuffer is shorter than {srcByteLen} bytes.");
                    }

                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    if (allValid)
                    {
                        Span<byte> dst = writer.GetSpan(srcByteLen);
                        src[..srcByteLen].CopyTo(dst);
                        writer.Advance(srcByteLen);
                    }
                    else
                    {
                        int outBytes = (nonNullCount + 7) >> 3;
                        Span<byte> dst = writer.GetSpan(outBytes);
                        dst[..outBytes].Clear();
                        int outBit = 0;
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (defLevels[i] == 0) continue;
                            if ((src[i >> 3] & (1 << (i & 7))) != 0)
                            {
                                dst[outBit >> 3] |= (byte)(1 << (outBit & 7));
                            }
                            outBit++;
                        }
                        writer.Advance(outBytes);
                    }
                    break;
                }

                case ArrowTypeId.Int8:
                {
                    if (col.DataBuffer.Length < rowCount)
                    {
                        throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer is shorter than {rowCount} bytes.");
                    }
                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    Span<byte> dst = writer.GetSpan(checked(nonNullCount * sizeof(int)));
                    int outIdx = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!allValid && defLevels[i] == 0) continue;
                        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(outIdx * 4, 4), unchecked((sbyte)src[i]));
                        outIdx++;
                    }
                    writer.Advance(outIdx * 4);
                    break;
                }

                case ArrowTypeId.UInt8:
                {
                    if (col.DataBuffer.Length < rowCount)
                    {
                        throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer is shorter than {rowCount} bytes.");
                    }
                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    Span<byte> dst = writer.GetSpan(checked(nonNullCount * sizeof(int)));
                    int outIdx = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!allValid && defLevels[i] == 0) continue;
                        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(outIdx * 4, 4), src[i]);
                        outIdx++;
                    }
                    writer.Advance(outIdx * 4);
                    break;
                }

                case ArrowTypeId.Int16:
                {
                    int needed = checked(rowCount * 2);
                    if (col.DataBuffer.Length < needed)
                    {
                        throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer is shorter than {needed} bytes.");
                    }
                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    Span<byte> dst = writer.GetSpan(checked(nonNullCount * sizeof(int)));
                    int outIdx = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!allValid && defLevels[i] == 0) continue;
                        short v = BinaryPrimitives.ReadInt16LittleEndian(src.Slice(i * 2, 2));
                        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(outIdx * 4, 4), v);
                        outIdx++;
                    }
                    writer.Advance(outIdx * 4);
                    break;
                }

                case ArrowTypeId.UInt16:
                {
                    int needed = checked(rowCount * 2);
                    if (col.DataBuffer.Length < needed)
                    {
                        throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer is shorter than {needed} bytes.");
                    }
                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    Span<byte> dst = writer.GetSpan(checked(nonNullCount * sizeof(int)));
                    int outIdx = 0;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!allValid && defLevels[i] == 0) continue;
                        ushort v = BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(i * 2, 2));
                        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(outIdx * 4, 4), v);
                        outIdx++;
                    }
                    writer.Advance(outIdx * 4);
                    break;
                }

                default:
                {
                    int elemWidth = col.Field.DataType.ByteWidth;
                    if (elemWidth <= 0)
                    {
                        ReadOnlySpan<byte> raw = col.DataBuffer.Span;
                        Span<byte> dst = writer.GetSpan(raw.Length);
                        raw.CopyTo(dst);
                        writer.Advance(raw.Length);
                        break;
                    }

                    int needed = checked(rowCount * elemWidth);
                    if (col.DataBuffer.Length < needed)
                    {
                        throw new ArgumentException($"Column '{col.Field.Name}' DataBuffer ({col.DataBuffer.Length}) is shorter than {needed} bytes.");
                    }

                    ReadOnlySpan<byte> src = col.DataBuffer.Span;
                    if (allValid)
                    {
                        Span<byte> dst = writer.GetSpan(needed);
                        src[..needed].CopyTo(dst);
                        writer.Advance(needed);
                    }
                    else
                    {
                        int outBytes = checked(nonNullCount * elemWidth);
                        Span<byte> dst = writer.GetSpan(outBytes);
                        int outOffset = 0;
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (defLevels[i] == 0) continue;
                            src.Slice(i * elemWidth, elemWidth).CopyTo(dst.Slice(outOffset, elemWidth));
                            outOffset += elemWidth;
                        }
                        writer.Advance(outOffset);
                    }
                    break;
                }
            }
        }
        finally
        {
            if (rentedDefLevels != null)
            {
                ArrayPool<uint>.Shared.Return(rentedDefLevels);
            }
        }
    }

    private static ParquetType MapArrowTypeToParquet(ArrowTypeId typeId) => typeId switch
    {
        ArrowTypeId.Boolean => ParquetType.Boolean,
        ArrowTypeId.Int8 or ArrowTypeId.UInt8 or ArrowTypeId.Int16 or ArrowTypeId.UInt16 or ArrowTypeId.Int32 or ArrowTypeId.UInt32 or ArrowTypeId.Date32 => ParquetType.Int32,
        ArrowTypeId.Int64 or ArrowTypeId.UInt64 or ArrowTypeId.Date64 or ArrowTypeId.Timestamp or ArrowTypeId.Duration or ArrowTypeId.Time64 => ParquetType.Int64,
        ArrowTypeId.Float => ParquetType.Float,
        ArrowTypeId.Double => ParquetType.Double,
        ArrowTypeId.Utf8 or ArrowTypeId.Binary => ParquetType.ByteArray,
        _ => ParquetType.ByteArray
    };
}
