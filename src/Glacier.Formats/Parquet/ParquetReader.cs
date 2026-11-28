namespace Glacier.Formats.Parquet;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Glacier.Formats.Arrow;

/// <summary>
/// High-performance Parquet v1 reader supporting <see cref="CompressionCodec.Uncompressed"/> and
/// <see cref="CompressionCodec.Snappy"/> with <c>PLAIN</c> value decoding, RLE definition levels, and multi-RowGroup merging.
/// </summary>
public static class ParquetReader
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;

    /// <summary>
    /// Reads a Parquet v1 file from <paramref name="filePath"/> into an <see cref="ArrowRecordBatch"/>.
    /// </summary>
    /// <param name="filePath">The source Parquet file path.</param>
    /// <returns>The decoded <see cref="ArrowRecordBatch"/>.</returns>
    public static ArrowRecordBatch ReadFile(string filePath)
    {
        using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        return ReadStream(fs);
    }

    /// <summary>
    /// Reads a Parquet v1 payload from an in-memory <see cref="ReadOnlyMemory{Byte}"/> buffer without copying the file.
    /// </summary>
    /// <param name="memory">The Parquet file bytes.</param>
    /// <returns>The decoded <see cref="ArrowRecordBatch"/>.</returns>
    public static ArrowRecordBatch ReadBytes(ReadOnlyMemory<byte> memory)
    {
        using var ms = new ReadOnlyMemoryStream(memory);
        return ReadStream(ms);
    }

    /// <summary>
    /// Reads a Parquet v1 file from a seekable <see cref="Stream"/> into an <see cref="ArrowRecordBatch"/>.
    /// </summary>
    /// <param name="stream">The readable seekable stream containing Parquet data.</param>
    /// <returns>The decoded <see cref="ArrowRecordBatch"/>.</returns>
    public static ArrowRecordBatch ReadStream(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (stream.Length < 12)
        {
            throw new InvalidDataException("Stream too small to be a valid Parquet file.");
        }

        // 1. Verify header magic
        stream.Seek(0, SeekOrigin.Begin);
        Span<byte> headMagic = stackalloc byte[4];
        stream.ReadExactly(headMagic);
        if (!headMagic.SequenceEqual(Magic))
        {
            throw new InvalidDataException("Invalid Parquet file: header magic 'PAR1' mismatch.");
        }

        // 2. Read footer magic and metadata length
        stream.Seek(stream.Length - 8, SeekOrigin.Begin);
        Span<byte> footer = stackalloc byte[8];
        stream.ReadExactly(footer);

        if (!footer.Slice(4, 4).SequenceEqual(Magic))
        {
            throw new InvalidDataException("Invalid Parquet file: footer magic 'PAR1' mismatch.");
        }

        uint metaLen = BinaryPrimitives.ReadUInt32LittleEndian(footer[..4]);
        long metaOffset = stream.Length - 8 - metaLen;
        if (metaOffset < 4 || metaLen > (uint)Array.MaxLength)
        {
            throw new InvalidDataException("Invalid Parquet metadata length.");
        }

        // 3. Read FileMetaData using pooled buffer + span Thrift reader
        stream.Seek(metaOffset, SeekOrigin.Begin);
        FileMetaData fileMeta;
        byte[] metaBytes = ArrayPool<byte>.Shared.Rent((int)metaLen);
        try
        {
            Span<byte> metaSpan = metaBytes.AsSpan(0, (int)metaLen);
            stream.ReadExactly(metaSpan);
            fileMeta = ParquetThriftCodec.ReadFileMetaData(metaSpan);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(metaBytes);
        }

        if (fileMeta.RowGroups.Count == 0 || fileMeta.Schema.Count <= 1)
        {
            return new ArrowRecordBatch(new ArrowSchema([]), 0, []);
        }

        // 4. Construct ArrowSchema from Parquet SchemaElements
        var fields = new List<ArrowField>(fileMeta.Schema.Count - 1);
        for (int i = 1; i < fileMeta.Schema.Count; i++)
        {
            var elem = fileMeta.Schema[i];
            var arrowType = MapParquetTypeToArrow(elem.Type ?? ParquetType.ByteArray);
            bool isNullable = elem.RepetitionType == FieldRepetitionType.Optional;
            fields.Add(new ArrowField(elem.Name, arrowType, isNullable));
        }

        var schema = new ArrowSchema(fields);
        int totalRows = checked((int)fileMeta.NumRows);
        if (totalRows < 0)
        {
            throw new InvalidDataException($"Invalid negative Parquet NumRows: {totalRows}.");
        }

        long rgRowSum = 0;
        for (int rgIdx = 0; rgIdx < fileMeta.RowGroups.Count; rgIdx++)
        {
            var rg = fileMeta.RowGroups[rgIdx];
            if (rg.NumRows < 0)
            {
                throw new InvalidDataException($"RowGroup {rgIdx} has negative NumRows: {rg.NumRows}.");
            }
            if (rg.Columns.Count < fields.Count)
            {
                throw new InvalidDataException($"RowGroup {rgIdx} column count ({rg.Columns.Count}) is less than schema field count ({fields.Count}).");
            }
            rgRowSum = checked(rgRowSum + rg.NumRows);
        }

        if (rgRowSum != totalRows)
        {
            throw new InvalidDataException($"Sum of RowGroup NumRows ({rgRowSum}) does not match FileMetaData.NumRows ({totalRows}).");
        }

        var columns = new ArrowColumn[fields.Count];
        for (int colIdx = 0; colIdx < fields.Count; colIdx++)
        {
            columns[colIdx] = DecodeColumnAcrossRowGroups(stream, fileMeta.RowGroups, colIdx, fields[colIdx], totalRows);
        }

        return new ArrowRecordBatch(schema, totalRows, columns);
    }

    private static ArrowColumn DecodeColumnAcrossRowGroups(
        Stream stream,
        List<RowGroup> rowGroups,
        int colIdx,
        ArrowField field,
        int totalRows)
    {
        bool isByteArray = field.DataType.Id is ArrowTypeId.Utf8 or ArrowTypeId.Binary;
        bool isBool = field.DataType.Id == ArrowTypeId.Boolean;
        int elemWidth = field.DataType.ByteWidth;

        byte[]? nullBitmap = null;
        int totalNullCount = 0;
        int rowCursor = 0;

        byte[]? offsetsBytes = isByteArray ? GC.AllocateUninitializedArray<byte>(checked((totalRows + 1) * sizeof(int))) : null;
        using var stringDataWriter = isByteArray ? new PooledBufferWriter(Math.Max(totalRows * 8, 256)) : null;
        byte[]? boolBytes = isBool ? new byte[(totalRows + 7) >> 3] : null;
        byte[]? fixedBytes = (!isByteArray && !isBool) ? new byte[checked(totalRows * elemWidth)] : null;

        var thriftReader = new ThriftCompactReader(stream);

        for (int rgIdx = 0; rgIdx < rowGroups.Count; rgIdx++)
        {
            var rg = rowGroups[rgIdx];
            int rgRows = checked((int)rg.NumRows);
            var chunk = rg.Columns[colIdx];
            var colMeta = chunk.MetaData ?? throw new InvalidDataException("Missing Parquet ColumnMetaData.");

            if (colMeta.DataPageOffset < 4 || colMeta.DataPageOffset >= stream.Length)
            {
                throw new InvalidDataException("Invalid Parquet DataPageOffset.");
            }

            stream.Seek(colMeta.DataPageOffset, SeekOrigin.Begin);
            int rgRowsRead = 0;

            do
            {
                var pageHeader = ParquetThriftCodec.ReadPageHeader(thriftReader);
                if (pageHeader.Type != PageType.DataPage || pageHeader.DataPageHeader == null)
                {
                    throw new InvalidDataException($"Expected DataPage header, got {pageHeader.Type}.");
                }

                var dph = pageHeader.DataPageHeader;
                if (dph.Encoding != ParquetEncoding.Plain)
                {
                    throw new NotSupportedException($"Unsupported Parquet page encoding '{dph.Encoding}'. Only PLAIN is supported.");
                }

                if (dph.NumValues < 0 || (long)rgRowsRead + dph.NumValues > rgRows)
                {
                    throw new InvalidDataException($"Invalid Parquet DataPage NumValues ({dph.NumValues}) for remaining RowGroup rows ({rgRows - rgRowsRead}).");
                }

                if (pageHeader.CompressedPageSize < 0 || pageHeader.UncompressedPageSize < 0)
                {
                    throw new InvalidDataException("Invalid negative Parquet page size.");
                }

                int compSize = pageHeader.CompressedPageSize;
                int uncompSize = pageHeader.UncompressedPageSize;
                byte[]? compBuf = compSize > 0 ? ArrayPool<byte>.Shared.Rent(compSize) : null;
                byte[]? uncompBuf = null;

                try
                {
                    Span<byte> compSpan = compBuf != null ? compBuf.AsSpan(0, compSize) : Span<byte>.Empty;
                    if (compSize > 0)
                    {
                        stream.ReadExactly(compSpan);
                    }

                    ReadOnlySpan<byte> pageSpan;
                    if (colMeta.Codec == CompressionCodec.Snappy)
                    {
                        uncompBuf = uncompSize > 0 ? ArrayPool<byte>.Shared.Rent(uncompSize) : null;
                        Span<byte> decompSpan = uncompBuf != null ? uncompBuf.AsSpan(0, uncompSize) : Span<byte>.Empty;
                        int decodedLen = SnappyCodec.Decompress(compSpan, decompSpan);
                        if (decodedLen != uncompSize)
                        {
                            throw new InvalidDataException(
                                $"Parquet Snappy page size mismatch: expected {uncompSize} bytes, decoded {decodedLen} bytes.");
                        }
                        pageSpan = decompSpan;
                    }
                    else if (colMeta.Codec == CompressionCodec.Uncompressed)
                    {
                        if (compSize != uncompSize)
                        {
                            throw new InvalidDataException("Uncompressed Parquet page size mismatch.");
                        }
                        pageSpan = compSpan;
                    }
                    else
                    {
                        throw new NotSupportedException($"Unsupported Parquet compression codec '{colMeta.Codec}'.");
                    }

                    int pageValues = dph.NumValues;
                    if (field.IsNullable)
                    {
                        if (dph.DefinitionLevelEncoding != ParquetEncoding.Rle)
                        {
                            throw new NotSupportedException($"Unsupported Parquet definition level encoding '{dph.DefinitionLevelEncoding}'.");
                        }

                        if (pageValues > 0)
                        {
                            if (pageSpan.Length < 4)
                            {
                                throw new InvalidDataException("Truncated Parquet RLE definition level length prefix.");
                            }

                            int rleLen = BinaryPrimitives.ReadInt32LittleEndian(pageSpan[..4]);
                            if (rleLen < 0 || 4L + rleLen > pageSpan.Length)
                            {
                                throw new InvalidDataException($"Invalid Parquet RLE definition level length ({rleLen}).");
                            }

                            uint[] defLevelsArr = ArrayPool<uint>.Shared.Rent(pageValues);
                            try
                            {
                                Span<uint> defLevels = defLevelsArr.AsSpan(0, pageValues);
                                int decodedLevels = ParquetRleCodec.Decode(pageSpan.Slice(4, rleLen), 1, defLevels);
                                if (decodedLevels != pageValues)
                                {
                                    throw new InvalidDataException($"Parquet definition level count mismatch: expected {pageValues}, decoded {decodedLevels}.");
                                }

                                int pageNulls = 0;
                                for (int r = 0; r < pageValues; r++)
                                {
                                    uint lvl = defLevels[r];
                                    if (lvl > 1u)
                                    {
                                        throw new InvalidDataException($"Corrupt Parquet definition level {lvl} for max_definition_level=1.");
                                    }
                                    if (lvl == 0u) pageNulls++;
                                }

                                if (pageNulls > 0)
                                {
                                    nullBitmap ??= AllocateBitmapWithLeadingOnes(totalRows, rowCursor);
                                    for (int r = 0; r < pageValues; r++)
                                    {
                                        if (defLevels[r] != 0u)
                                        {
                                            int globalRow = rowCursor + r;
                                            nullBitmap[globalRow >> 3] |= (byte)(1 << (globalRow & 7));
                                        }
                                    }
                                    totalNullCount += pageNulls;
                                }
                                else if (nullBitmap != null)
                                {
                                    MarkRangeValid(nullBitmap, rowCursor, pageValues);
                                }

                                ReadOnlySpan<byte> valuesSpan = pageSpan.Slice(4 + rleLen);
                                int nonNullCount = pageValues - pageNulls;
                                DecodePlainValues(
                                    field,
                                    valuesSpan,
                                    pageValues,
                                    nonNullCount,
                                    pageNulls > 0 ? defLevels : ReadOnlySpan<uint>.Empty,
                                    rowCursor,
                                    offsetsBytes,
                                    stringDataWriter,
                                    boolBytes,
                                    fixedBytes);
                            }
                            finally
                            {
                                ArrayPool<uint>.Shared.Return(defLevelsArr);
                            }
                        }
                    }
                    else
                    {
                        if (pageValues > 0)
                        {
                            DecodePlainValues(
                                field,
                                pageSpan,
                                pageValues,
                                pageValues,
                                ReadOnlySpan<uint>.Empty,
                                rowCursor,
                                offsetsBytes,
                                stringDataWriter,
                                boolBytes,
                                fixedBytes);
                        }
                    }

                    rowCursor += pageValues;
                    rgRowsRead += pageValues;
                }
                finally
                {
                    if (uncompBuf != null) ArrayPool<byte>.Shared.Return(uncompBuf);
                    if (compBuf != null) ArrayPool<byte>.Shared.Return(compBuf);
                }
            } while (rgRowsRead < rgRows);
        }

        ReadOnlyMemory<byte> nullMemory = totalNullCount > 0 ? nullBitmap! : ReadOnlyMemory<byte>.Empty;

        if (isByteArray)
        {
            MemoryMarshal.Cast<byte, int>(offsetsBytes!.AsSpan())[totalRows] = stringDataWriter!.WrittenCount;
            byte[] dataBytes = stringDataWriter.WrittenCount == 0 ? [] : stringDataWriter.WrittenSpan.ToArray();
            return new ArrowColumn(field, totalRows, totalNullCount, nullMemory, offsetsBytes!, dataBytes);
        }

        if (isBool)
        {
            return new ArrowColumn(field, totalRows, totalNullCount, nullMemory, ReadOnlyMemory<byte>.Empty, boolBytes!);
        }

        return new ArrowColumn(field, totalRows, totalNullCount, nullMemory, ReadOnlyMemory<byte>.Empty, fixedBytes!);
    }

    private static void DecodePlainValues(
        ArrowField field,
        ReadOnlySpan<byte> valuesSpan,
        int pageValues,
        int nonNullCount,
        ReadOnlySpan<uint> defLevels,
        int rowCursor,
        byte[]? offsetsBytes,
        PooledBufferWriter? stringDataWriter,
        byte[]? boolBytes,
        byte[]? fixedBytes)
    {
        bool hasNulls = !defLevels.IsEmpty;

        if (field.DataType.Id is ArrowTypeId.Utf8 or ArrowTypeId.Binary)
        {
            Span<int> offsets = MemoryMarshal.Cast<byte, int>(offsetsBytes!.AsSpan());
            int srcOffset = 0;

            for (int r = 0; r < pageValues; r++)
            {
                offsets[rowCursor + r] = stringDataWriter!.WrittenCount;
                if (hasNulls && defLevels[r] == 0u)
                {
                    continue;
                }

                if (srcOffset + 4 > valuesSpan.Length)
                {
                    throw new InvalidDataException("Truncated PLAIN ByteArray length prefix in Parquet page.");
                }

                int len = BinaryPrimitives.ReadInt32LittleEndian(valuesSpan.Slice(srcOffset, 4));
                srcOffset += 4;
                if (len < 0 || (long)srcOffset + len > valuesSpan.Length)
                {
                    throw new InvalidDataException("Invalid PLAIN ByteArray element length in Parquet page.");
                }

                if (len > 0)
                {
                    Span<byte> dst = stringDataWriter.GetSpan(len);
                    valuesSpan.Slice(srcOffset, len).CopyTo(dst);
                    stringDataWriter.Advance(len);
                    srcOffset += len;
                }
            }
            return;
        }

        if (field.DataType.Id == ArrowTypeId.Boolean)
        {
            int neededBytes = (nonNullCount + 7) >> 3;
            if (valuesSpan.Length < neededBytes)
            {
                throw new InvalidDataException("Truncated Parquet boolean column page.");
            }

            if (!hasNulls && (rowCursor & 7) == 0)
            {
                valuesSpan[..neededBytes].CopyTo(boolBytes!.AsSpan(rowCursor >> 3));
                return;
            }

            int srcBit = 0;
            for (int r = 0; r < pageValues; r++)
            {
                if (hasNulls && defLevels[r] == 0u) continue;
                bool bit = (valuesSpan[srcBit >> 3] & (1 << (srcBit & 7))) != 0;
                srcBit++;
                if (bit)
                {
                    int globalRow = rowCursor + r;
                    boolBytes![globalRow >> 3] |= (byte)(1 << (globalRow & 7));
                }
            }
            return;
        }

        int elemWidth = field.DataType.ByteWidth;
        int neededFixed = checked(nonNullCount * elemWidth);
        if (valuesSpan.Length < neededFixed)
        {
            throw new InvalidDataException("Truncated Parquet fixed-width column page.");
        }

        if (!hasNulls)
        {
            valuesSpan[..neededFixed].CopyTo(fixedBytes!.AsSpan(checked(rowCursor * elemWidth)));
            return;
        }

        int srcByte = 0;
        for (int r = 0; r < pageValues; r++)
        {
            if (defLevels[r] == 0u) continue;
            int dstByte = checked((rowCursor + r) * elemWidth);
            valuesSpan.Slice(srcByte, elemWidth).CopyTo(fixedBytes!.AsSpan(dstByte, elemWidth));
            srcByte += elemWidth;
        }
    }

    private static byte[] AllocateBitmapWithLeadingOnes(int totalRows, int validPrefixRows)
    {
        byte[] bitmap = new byte[(totalRows + 7) >> 3];
        MarkRangeValid(bitmap, 0, validPrefixRows);
        return bitmap;
    }

    private static void MarkRangeValid(byte[] bitmap, int startRow, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int row = startRow + i;
            bitmap[row >> 3] |= (byte)(1 << (row & 7));
        }
    }

    private static ArrowType MapParquetTypeToArrow(ParquetType pType) => pType switch
    {
        ParquetType.Boolean => ArrowType.Boolean,
        ParquetType.Int32 => ArrowType.Int32,
        ParquetType.Int64 => ArrowType.Int64,
        ParquetType.Float => ArrowType.Float,
        ParquetType.Double => ArrowType.Double,
        ParquetType.ByteArray => ArrowType.Utf8,
        _ => ArrowType.Utf8
    };
}
