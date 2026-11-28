namespace Glacier.Formats.Tests;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Glacier.Formats.Arrow;
using Glacier.Formats.Parquet;
using Xunit;

public class HardeningAndNullableParquetTests
{
    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    public void Parquet_NullableAllTypes_PartialAndAllNulls_Roundtrips(CompressionCodec codec)
    {
        const int rows = 23;
        var schema = new ArrowSchema([
            new ArrowField("b", ArrowType.Boolean, isNullable: true),
            new ArrowField("i32", ArrowType.Int32, isNullable: true),
            new ArrowField("i64", ArrowType.Int64, isNullable: true),
            new ArrowField("f32", ArrowType.Float, isNullable: true),
            new ArrowField("f64", ArrowType.Double, isNullable: true),
            new ArrowField("str", ArrowType.Utf8, isNullable: true),
            new ArrowField("all_null", ArrowType.Int32, isNullable: true),
            new ArrowField("no_null_opt", ArrowType.Int32, isNullable: true)
        ]);

        byte[] boolData = new byte[(rows + 7) >> 3];
        byte[] boolNulls = new byte[(rows + 7) >> 3];
        int boolNullCount = 0;

        int[] i32 = new int[rows];
        long[] i64 = new long[rows];
        float[] f32 = new float[rows];
        double[] f64 = new double[rows];
        byte[] valNulls = new byte[(rows + 7) >> 3];
        int valNullCount = 0;

        int[] strOffsets = new int[rows + 1];
        using var strMs = new MemoryStream();
        byte[] allNullBitmap = new byte[(rows + 7) >> 3]; // all 0 -> all null
        int[] noNullOpt = new int[rows];

        for (int i = 0; i < rows; i++)
        {
            if (i % 3 != 0)
            {
                boolNulls[i >> 3] |= (byte)(1 << (i & 7));
                if (i % 2 == 0) boolData[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                boolNullCount++;
            }

            if (i % 4 != 1)
            {
                valNulls[i >> 3] |= (byte)(1 << (i & 7));
                i32[i] = i * 100 - 50;
                i64[i] = 9_000_000_000L + i;
                f32[i] = i * 0.25f;
                f64[i] = i * -2.5;
                strOffsets[i] = (int)strMs.Position;
                strMs.Write(Encoding.UTF8.GetBytes($"item_{i}"));
            }
            else
            {
                valNullCount++;
                strOffsets[i] = (int)strMs.Position;
            }

            noNullOpt[i] = i + 1000;
        }
        strOffsets[rows] = (int)strMs.Position;

        var batch = new ArrowRecordBatch(schema, rows, [
            new ArrowColumn(schema.GetField(0), rows, boolNullCount, boolNulls, ReadOnlyMemory<byte>.Empty, boolData),
            new ArrowColumn(schema.GetField(1), rows, valNullCount, valNulls, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(i32.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(2), rows, valNullCount, valNulls, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(i64.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(3), rows, valNullCount, valNulls, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(f32.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(4), rows, valNullCount, valNulls, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(f64.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(5), rows, valNullCount, valNulls, MemoryMarshal.AsBytes(strOffsets.AsSpan()).ToArray(), strMs.ToArray()),
            new ArrowColumn(schema.GetField(6), rows, rows, allNullBitmap, ReadOnlyMemory<byte>.Empty, new byte[rows * 4]),
            new ArrowColumn(schema.GetField(7), rows, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(noNullOpt.AsSpan()).ToArray())
        ]);

        byte[] parquet = ParquetWriter.WriteToBytes(batch, codec);
        var read = ParquetReader.ReadBytes(parquet);

        Assert.Equal(rows, read.RowCount);
        Assert.Equal(boolNullCount, read.Column(0).NullCount);
        Assert.Equal(valNullCount, read.Column(1).NullCount);
        Assert.Equal(valNullCount, read.Column(5).NullCount);
        Assert.Equal(rows, read.Column(6).NullCount);
        Assert.Equal(0, read.Column(7).NullCount);

        for (int i = 0; i < rows; i++)
        {
            Assert.Equal(i % 3 == 0, read.Column(0).IsNull(i));
            if (i % 3 != 0) Assert.Equal(i % 2 == 0, read.Column(0).GetBoolean(i));

            bool isNull = i % 4 == 1;
            Assert.Equal(isNull, read.Column(1).IsNull(i));
            Assert.Equal(isNull, read.Column(2).IsNull(i));
            Assert.Equal(isNull, read.Column(3).IsNull(i));
            Assert.Equal(isNull, read.Column(4).IsNull(i));
            Assert.Equal(isNull, read.Column(5).IsNull(i));

            if (!isNull)
            {
                Assert.Equal(i * 100 - 50, read.Column(1).GetInt32(i));
                Assert.Equal(9_000_000_000L + i, read.Column(2).GetInt64(i));
                Assert.Equal(i * 0.25f, read.Column(3).GetFloat(i), 4);
                Assert.Equal(i * -2.5, read.Column(4).GetDouble(i), 4);
                Assert.Equal($"item_{i}", read.Column(5).GetString(i));
            }
            else
            {
                Assert.Equal(string.Empty, read.Column(5).GetString(i));
            }

            Assert.True(read.Column(6).IsNull(i));
            Assert.False(read.Column(7).IsNull(i));
            Assert.Equal(i + 1000, read.Column(7).GetInt32(i));
        }

        // Verify CreatedBy footer string is updated from Glacier.Storage to Glacier.Formats
        uint metaLen = BinaryPrimitives.ReadUInt32LittleEndian(parquet.AsSpan(parquet.Length - 8, 4));
        using var metaMs = new MemoryStream(parquet, parquet.Length - 8 - (int)metaLen, (int)metaLen);
        var meta = ParquetThriftCodec.ReadFileMetaData(new ThriftCompactReader(metaMs));
        Assert.StartsWith("Glacier.Formats", meta.CreatedBy);
        Assert.DoesNotContain("Glacier.Storage", meta.CreatedBy);
    }

    [Fact]
    public void Parquet_MultiRowGroup_MixedNullsAndZeroNulls_PreservesAllNullsAndValues()
    {
        var schema = new ArrowSchema([
            new ArrowField("id", ArrowType.Int32, isNullable: true),
            new ArrowField("flag", ArrowType.Boolean, isNullable: true),
            new ArrowField("tag", ArrowType.Utf8, isNullable: true)
        ]);

        // RG 1: 4 rows, 0 nulls
        var b1 = MakeNullableBatch(schema, [1, 2, 3, 4], [true, false, true, false], ["a", "b", "c", "d"]);
        // RG 2: 5 rows, partial nulls
        var b2 = MakeNullableBatch(schema, [null, 20, null, 40, null], [false, null, true, null, false], [null, "x", null, "y", null]);
        // RG 3: 3 rows, all nulls
        var b3 = MakeNullableBatch(schema, [null, null, null], [null, null, null], [null, null, null]);
        // RG 4: 2 rows, 0 nulls
        var b4 = MakeNullableBatch(schema, [100, 200], [true, true], ["end1", "end2"]);

        using var ms = new MemoryStream();
        ParquetWriter.WriteStream(ms, [b1, b2, b3, b4], CompressionCodec.Snappy);
        ms.Seek(0, SeekOrigin.Begin);

        var read = ParquetReader.ReadStream(ms);
        Assert.Equal(14, read.RowCount);
        Assert.Equal(6, read.Column(0).NullCount);
        Assert.Equal(5, read.Column(1).NullCount);
        Assert.Equal(6, read.Column(2).NullCount);

        int?[] expectedIds = [1, 2, 3, 4, null, 20, null, 40, null, null, null, null, 100, 200];
        bool?[] expectedFlags = [true, false, true, false, false, null, true, null, false, null, null, null, true, true];
        string?[] expectedTags = ["a", "b", "c", "d", null, "x", null, "y", null, null, null, null, "end1", "end2"];

        for (int i = 0; i < 14; i++)
        {
            Assert.Equal(expectedIds[i] is null, read.Column(0).IsNull(i));
            if (expectedIds[i] is int idVal) Assert.Equal(idVal, read.Column(0).GetInt32(i));

            Assert.Equal(expectedFlags[i] is null, read.Column(1).IsNull(i));
            if (expectedFlags[i] is bool bVal) Assert.Equal(bVal, read.Column(1).GetBoolean(i));

            Assert.Equal(expectedTags[i] is null, read.Column(2).IsNull(i));
            if (expectedTags[i] is string sVal) Assert.Equal(sVal, read.Column(2).GetString(i));
        }
    }

    [Fact]
    public void ParquetReader_CorruptDefinitionLevelGreaterThanOne_ThrowsInvalidDataException()
    {
        // Craft a valid Optional Int32 Parquet file, but replace RLE def levels with value 2
        byte[] badDefLevels = [0b0000_0100, 0x02]; // RLE run: count=2, value=2 (> max_definition_level=1)
        using var pagePayload = new MemoryStream();
        Span<byte> lenPrefix = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, badDefLevels.Length);
        pagePayload.Write(lenPrefix);
        pagePayload.Write(badDefLevels);
        pagePayload.Write([1, 0, 0, 0, 2, 0, 0, 0]);
        byte[] rawPage = pagePayload.ToArray();

        using var ms = new MemoryStream();
        ms.Write("PAR1"u8);
        long chunkOffset = ms.Position;
        var ph = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = rawPage.Length,
            CompressedPageSize = rawPage.Length,
            DataPageHeader = new DataPageHeader
            {
                NumValues = 2,
                Encoding = ParquetEncoding.Plain,
                DefinitionLevelEncoding = ParquetEncoding.Rle,
                RepetitionLevelEncoding = ParquetEncoding.Rle
            }
        };
        ParquetThriftCodec.WritePageHeader(new ThriftCompactWriter(ms), ph);
        ms.Write(rawPage);

        var fm = new FileMetaData
        {
            Version = 1,
            Schema = [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement { Name = "x", Type = ParquetType.Int32, RepetitionType = FieldRepetitionType.Optional }
            ],
            NumRows = 2,
            RowGroups = [
                new RowGroup
                {
                    NumRows = 2,
                    TotalByteSize = rawPage.Length,
                    Columns = [
                        new ColumnChunk
                        {
                            FileOffset = chunkOffset,
                            MetaData = new ColumnMetaData
                            {
                                Type = ParquetType.Int32,
                                Encodings = [ParquetEncoding.Rle, ParquetEncoding.Plain],
                                PathInSchema = ["x"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 2,
                                TotalUncompressedSize = rawPage.Length,
                                TotalCompressedSize = rawPage.Length,
                                DataPageOffset = chunkOffset
                            }
                        }
                    ]
                }
            ]
        };

        using var metaMs = new MemoryStream();
        ParquetThriftCodec.WriteFileMetaData(new ThriftCompactWriter(metaMs), fm);
        byte[] metaBytes = metaMs.ToArray();
        ms.Write(metaBytes);
        BinaryPrimitives.WriteUInt32LittleEndian(lenPrefix, (uint)metaBytes.Length);
        ms.Write(lenPrefix);
        ms.Write("PAR1"u8);

        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(ms.ToArray()));
    }

    [Fact]
    public void ParquetRleCodec_UniformRleCompressionAndAdversarialBounds()
    {
        // Uniform 10,000-element run compresses to a single varint RLE run (<= 6 bytes)
        uint[] uniform = new uint[10_000];
        Array.Fill(uniform, 1u);
        byte[] enc = new byte[ParquetRleCodec.GetMaxEncodedLength(uniform.Length, 1)];
        int encLen = ParquetRleCodec.Encode(uniform, 1, enc);
        Assert.InRange(encLen, 2, 6);

        uint[] dec = new uint[10_000];
        Assert.Equal(10_000, ParquetRleCodec.Decode(enc.AsSpan(0, encLen), 1, dec));
        Assert.All(dec, v => Assert.Equal(1u, v));

        // Alternating 8-equal and 8-mixed runs verify GetMaxEncodedLength bound holds
        uint[] mixed = new uint[512];
        for (int g = 0; g < 64; g++)
        {
            for (int k = 0; k < 8; k++)
            {
                mixed[g * 8 + k] = (g % 2 == 0) ? 1u : (uint)(k & 1);
            }
        }
        int maxBound = ParquetRleCodec.GetMaxEncodedLength(mixed.Length, 1);
        byte[] mixedBuf = new byte[maxBound];
        int mixedLen = ParquetRleCodec.Encode(mixed, 1, mixedBuf);
        Assert.True(mixedLen <= maxBound);
        uint[] mixedDec = new uint[512];
        Assert.Equal(512, ParquetRleCodec.Decode(mixedBuf.AsSpan(0, mixedLen), 1, mixedDec));
        Assert.Equal(mixed, mixedDec);

        // Overflow numGroups * bitWidth -> InvalidDataException
        // header = (0x10000000 << 1) | 1 = 0x20000001 -> varint: 81 80 80 80 02
        byte[] overflowRun = [0x81, 0x80, 0x80, 0x80, 0x02];
        Assert.Throws<InvalidDataException>(() => ParquetRleCodec.Decode(overflowRun, 16, new uint[8]));

        // Zero-group bit-packed header (0x01) -> InvalidDataException
        Assert.Throws<InvalidDataException>(() => ParquetRleCodec.Decode([0x01], 1, new uint[8]));

        // Truncated RLE value -> InvalidDataException
        Assert.Throws<InvalidDataException>(() => ParquetRleCodec.Decode([0x04], 8, new uint[2]));

        // Truncated varint -> InvalidDataException
        Assert.Throws<InvalidDataException>(() => ParquetRleCodec.Decode([0x80], 1, new uint[2]));

        // Invalid bitWidth
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetRleCodec.Decode([], -1, new uint[1]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetRleCodec.Decode([], 33, new uint[1]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetRleCodec.Encode(uniform, -1, enc));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetRleCodec.Encode(uniform, 33, enc));
    }

    [Fact]
    public void ThriftCompactProtocol_InvalidWireTypesOverlongVarintsAndSeekSkip()
    {
        // Unknown ThriftType 13..15 in ReadFieldBegin -> InvalidDataException
        byte[] badFieldType = [0x1D]; // delta=1, type=13
        var r1 = new ThriftCompactReader(new MemoryStream(badFieldType));
        Assert.Throws<InvalidDataException>(() => r1.ReadFieldBegin(out _, out _));

        // Direct Skip(13..15) -> InvalidDataException
        var r2 = new ThriftCompactReader(new MemoryStream());
        Assert.Throws<InvalidDataException>(() => r2.Skip((ThriftType)14));

        // Overlong 6-byte varint32 -> InvalidDataException
        byte[] overlong32 = [0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        var r3 = new ThriftCompactReader(new MemoryStream(overlong32));
        Assert.Throws<InvalidDataException>(() => r3.ReadI32());

        // Overlong 11-byte varint64 -> InvalidDataException
        byte[] overlong64 = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        var r4 = new ThriftCompactReader(new MemoryStream(overlong64));
        Assert.Throws<InvalidDataException>(() => r4.ReadI64());

        // Negative list size -> ArgumentOutOfRangeException
        var w = new ThriftCompactWriter(new MemoryStream());
        Assert.Throws<ArgumentOutOfRangeException>(() => w.WriteListBegin(ThriftType.I32, -1));

        // Large string (> 512 bytes) + Skip(Binary) on non-seekable stream
        string longText = new('Z', 1200);
        using var ms = new MemoryStream();
        var writer = new ThriftCompactWriter(ms);
        writer.WriteStructBegin();
        writer.WriteFieldBegin(1, ThriftType.Binary);
        writer.WriteString(longText);
        writer.WriteFieldBegin(2, ThriftType.Binary);
        writer.WriteString(longText);
        writer.WriteFieldBegin(3, ThriftType.I32);
        writer.WriteI32(777);
        writer.WriteStructEnd();

        ms.Position = 0;
        using var nonSeek = new NonSeekableStreamWrapper(ms);
        var reader = new ThriftCompactReader(nonSeek);
        reader.ReadStructBegin();
        Assert.True(reader.ReadFieldBegin(out _, out var t1));
        Assert.Equal(longText, reader.ReadString());
        Assert.True(reader.ReadFieldBegin(out _, out var t2));
        reader.Skip(t2);
        Assert.True(reader.ReadFieldBegin(out _, out _));
        Assert.Equal(777, reader.ReadI32());
        Assert.False(reader.ReadFieldBegin(out _, out _));
    }

    [Fact]
    public void ArrowIpc_HardeningBoundsAndZeroCopyApis_WorkCorrectly()
    {
        // Field name > 255 UTF-8 bytes -> ArgumentException
        var longFieldSchema = new ArrowSchema([new ArrowField(new string('a', 256), ArrowType.Int32)]);
        using var sw = new ArrowStreamWriter(new MemoryStream());
        Assert.Throws<ArgumentException>(() => sw.WriteSchema(longFieldSchema));

        // ArrowIpcParser invalid inputs
        Assert.False(ArrowIpcParser.TryReadPrefix(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, out _)); // negative metadataLength
        Assert.False(ArrowIpcParser.TryReadPrefix(new byte[] { 0x00, 0x00, 0x00, 0x80, 0, 0, 0, 0 }, out _)); // > int.MaxValue

        // IBufferWriter + ReadSchema(span) + ReadRecordBatch(memory) zero-copy path
        var schema = new ArrowSchema([
            new ArrowField("id", ArrowType.Int32, isNullable: false),
            new ArrowField("txt", ArrowType.Utf8, isNullable: true)
        ]);
        int[] ids = [10, 20, 30];
        int[] offsets = [0, 2, 2, 5];
        byte[] strBytes = Encoding.UTF8.GetBytes("hiabc");
        byte[] nulls = [0b0000_0101]; // index 1 null
        var batch = new ArrowRecordBatch(schema, 3, [
            new ArrowColumn(schema.GetField(0), 3, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(ids.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(1), 3, 1, nulls, MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray(), strBytes)
        ]);

        var abw = new ArrayBufferWriter<byte>();
        ArrowStreamWriter.WriteSchema(abw, schema);
        ArrowStreamWriter.WriteRecordBatch(abw, batch);
        ArrowStreamWriter.WriteEndOfStream(abw);

        ReadOnlyMemory<byte> fullMem = abw.WrittenMemory;
        var parsedSchema = ArrowStreamReader.ReadSchema(fullMem.Span, out int schemaConsumed);
        Assert.Equal(2, parsedSchema.FieldCount);

        var parsedBatch = ArrowStreamReader.ReadRecordBatch(fullMem.Slice(schemaConsumed), parsedSchema, out int batchConsumed);
        Assert.NotNull(parsedBatch);
        Assert.Equal(3, parsedBatch.RowCount);
        Assert.Equal(20, parsedBatch.Column(0).GetInt32(1));
        Assert.True(parsedBatch.Column(1).IsNull(1));
        Assert.Equal("abc", parsedBatch.Column(1).GetString(2));

        var eosBatch = ArrowStreamReader.ReadRecordBatch(fullMem.Slice(schemaConsumed + batchConsumed), parsedSchema, out int eosConsumed);
        Assert.Null(eosBatch);
        Assert.Equal(8, eosConsumed);

        // Full slice returns same instance (0 allocations)
        Assert.Same(batch, batch.Slice(0, 3));
        Assert.Same(batch.Column(1), batch.Column(1).Slice(0, 3));

        // Empty-column batch Slice validates bounds
        var emptyColBatch = new ArrowRecordBatch(new ArrowSchema([]), 5, Array.Empty<ArrowColumn>());
        Assert.Throws<ArgumentOutOfRangeException>(() => emptyColBatch.Slice(3, 4));
    }

    [Theory]
    [InlineData(FieldRepetitionType.Required)]
    [InlineData(FieldRepetitionType.Optional)]
    public void ParquetReader_MultiPageDataPageNumValuesInt32Overflow_ThrowsInvalidDataException(FieldRepetitionType repetitionType)
    {
        bool isOptional = repetitionType == FieldRepetitionType.Optional;
        byte[] page1Raw;
        if (isOptional)
        {
            byte[] def1 = [0b0000_0010, 0x01]; // RLE run: count=1, value=1 (non-null)
            page1Raw = new byte[4 + def1.Length + 4];
            BinaryPrimitives.WriteInt32LittleEndian(page1Raw.AsSpan(0, 4), def1.Length);
            def1.CopyTo(page1Raw.AsSpan(4));
            BinaryPrimitives.WriteInt32LittleEndian(page1Raw.AsSpan(4 + def1.Length, 4), 42);
        }
        else
        {
            page1Raw = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(page1Raw, 42);
        }

        using var ms = new MemoryStream();
        ms.Write("PAR1"u8);
        long chunkOffset = ms.Position;

        var ph1 = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = page1Raw.Length,
            CompressedPageSize = page1Raw.Length,
            DataPageHeader = new DataPageHeader
            {
                NumValues = 1,
                Encoding = ParquetEncoding.Plain,
                DefinitionLevelEncoding = ParquetEncoding.Rle,
                RepetitionLevelEncoding = ParquetEncoding.Rle
            }
        };
        ParquetThriftCodec.WritePageHeader(new ThriftCompactWriter(ms), ph1);
        ms.Write(page1Raw);

        // Page 2 claims int.MaxValue values so unchecked (1 + int.MaxValue) would wrap negative <= 10
        var ph2 = new PageHeader
        {
            Type = PageType.DataPage,
            UncompressedPageSize = 0,
            CompressedPageSize = 0,
            DataPageHeader = new DataPageHeader
            {
                NumValues = int.MaxValue,
                Encoding = ParquetEncoding.Plain,
                DefinitionLevelEncoding = ParquetEncoding.Rle,
                RepetitionLevelEncoding = ParquetEncoding.Rle
            }
        };
        ParquetThriftCodec.WritePageHeader(new ThriftCompactWriter(ms), ph2);

        var fm = new FileMetaData
        {
            Version = 1,
            Schema = [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement { Name = "x", Type = ParquetType.Int32, RepetitionType = repetitionType }
            ],
            NumRows = 10,
            RowGroups = [
                new RowGroup
                {
                    NumRows = 10,
                    TotalByteSize = page1Raw.Length,
                    Columns = [
                        new ColumnChunk
                        {
                            FileOffset = chunkOffset,
                            MetaData = new ColumnMetaData
                            {
                                Type = ParquetType.Int32,
                                Encodings = isOptional ? [ParquetEncoding.Rle, ParquetEncoding.Plain] : [ParquetEncoding.Plain],
                                PathInSchema = ["x"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 10,
                                TotalUncompressedSize = page1Raw.Length,
                                TotalCompressedSize = page1Raw.Length,
                                DataPageOffset = chunkOffset
                            }
                        }
                    ]
                }
            ]
        };

        using var metaMs = new MemoryStream();
        ParquetThriftCodec.WriteFileMetaData(new ThriftCompactWriter(metaMs), fm);
        byte[] metaBytes = metaMs.ToArray();
        ms.Write(metaBytes);
        Span<byte> lenPrefix = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lenPrefix, (uint)metaBytes.Length);
        ms.Write(lenPrefix);
        ms.Write("PAR1"u8);

        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(ms.ToArray()));
    }

    [Fact]
    public void ArrowStreamReader_CorruptDescriptorsAndOffsets_ThrowInvalidDataException()
    {
        // 1. Non-monotonic OffsetsBuffer [0, 5, 2, 6]
        var utf8Schema = new ArrowSchema([new ArrowField("s", ArrowType.Utf8, isNullable: true)]);
        int[] badMonoOffsets = [0, 5, 2, 6];
        byte[] utf8Body6 = "abcdef"u8.ToArray();
        byte[] frameNonMono = BuildSyntheticIpcBatchFrame(
            utf8Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 16, 16, utf8Body6.Length)],
            [.. MemoryMarshal.AsBytes(badMonoOffsets.AsSpan()), .. utf8Body6]);
        AssertBatchRejectedBothPaths(frameNonMono);

        // 2. Out-of-bounds OffsetsBuffer final offset > DataLength
        int[] badOobOffsets = [0, 1, 2, 99];
        byte[] frameOobOffsets = BuildSyntheticIpcBatchFrame(
            utf8Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 16, 16, utf8Body6.Length)],
            [.. MemoryMarshal.AsBytes(badOobOffsets.AsSpan()), .. utf8Body6]);
        AssertBatchRejectedBothPaths(frameOobOffsets);

        // 3. Negative initial offset [-1, 1, 2, 3]
        int[] badNegOffsets = [-1, 1, 2, 3];
        byte[] frameNegOffsets = BuildSyntheticIpcBatchFrame(
            utf8Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 16, 16, utf8Body6.Length)],
            [.. MemoryMarshal.AsBytes(badNegOffsets.AsSpan()), .. utf8Body6]);
        AssertBatchRejectedBothPaths(frameNegOffsets);

        // 4. NullCount = 2 with NullBitmapLength = 0
        var i32Schema = new ArrowSchema([new ArrowField("x", ArrowType.Int32, isNullable: true)]);
        byte[] frameMissingNullBitmap = BuildSyntheticIpcBatchFrame(
            i32Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 2, 0, 0, 0, 0, 0, 12)],
            new byte[12]);
        AssertBatchRejectedBothPaths(frameMissingNullBitmap);

        // 5. Utf8 / Binary Length = 3 with OffsetsLength = 0
        byte[] frameZeroOffsets = BuildSyntheticIpcBatchFrame(
            utf8Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 0, 0, 6)],
            utf8Body6);
        AssertBatchRejectedBothPaths(frameZeroOffsets);

        var binSchema = new ArrowSchema([new ArrowField("b", ArrowType.Binary, isNullable: true)]);
        byte[] frameZeroBinOffsets = BuildSyntheticIpcBatchFrame(
            binSchema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 0, 0, 6)],
            utf8Body6);
        AssertBatchRejectedBothPaths(frameZeroBinOffsets);

        // 6. Int32 & Boolean Length = 3 with DataLength = 0
        byte[] frameZeroIntData = BuildSyntheticIpcBatchFrame(
            i32Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 0, 0, 0)],
            []);
        AssertBatchRejectedBothPaths(frameZeroIntData);

        var boolSchema = new ArrowSchema([new ArrowField("flag", ArrowType.Boolean, isNullable: false)]);
        byte[] frameZeroBoolData = BuildSyntheticIpcBatchFrame(
            boolSchema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 3, 0, 0, 0, 0, 0, 0, 0)],
            []);
        AssertBatchRejectedBothPaths(frameZeroBoolData);

        // 7. Descriptor Length = 2 != batch RowCount = 3
        byte[] frameLenMismatch = BuildSyntheticIpcBatchFrame(
            i32Schema, rowCount: 3,
            [new ArrowColumnDescriptor(0, 2, 0, 0, 0, 0, 0, 0, 12)],
            new byte[12]);
        AssertBatchRejectedBothPaths(frameLenMismatch);

        // 8. Duplicate FieldIndex in two-column schema
        var twoColSchema = new ArrowSchema([
            new ArrowField("a", ArrowType.Int32, isNullable: false),
            new ArrowField("b", ArrowType.Int32, isNullable: false)
        ]);
        byte[] frameDupField = BuildSyntheticIpcBatchFrame(
            twoColSchema, rowCount: 2,
            [
                new ArrowColumnDescriptor(0, 2, 0, 0, 0, 0, 0, 0, 8),
                new ArrowColumnDescriptor(0, 2, 0, 0, 0, 0, 0, 0, 8)
            ],
            new byte[8]);
        AssertBatchRejectedBothPaths(frameDupField);

        // 9. ColumnCount != Schema.FieldCount
        byte[] frameWrongColCount = BuildSyntheticIpcBatchFrame(
            twoColSchema, rowCount: 2,
            [new ArrowColumnDescriptor(0, 2, 0, 0, 0, 0, 0, 0, 8)],
            new byte[8]);
        AssertBatchRejectedBothPaths(frameWrongColCount);
    }

    [Fact]
    public void ArrowStreamReader_InvalidStreamMessageTypes_ThrowInvalidDataException()
    {
        var schema = new ArrowSchema([new ArrowField("id", ArrowType.Int32, isNullable: false)]);
        int[] vals = [1, 2];
        var batch = new ArrowRecordBatch(schema, 2, [
            new ArrowColumn(schema.GetField(0), 2, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(vals.AsSpan()).ToArray())
        ]);

        // Stream starting with RecordBatch instead of Schema -> ReadSchema throws InvalidDataException
        var abwBatchFirst = new ArrayBufferWriter<byte>();
        ArrowStreamWriter.WriteRecordBatch(abwBatchFirst, batch);
        using (var reader = new ArrowStreamReader(new MemoryStream(abwBatchFirst.WrittenSpan.ToArray())))
        {
            Assert.Throws<InvalidDataException>(() => reader.ReadSchema());
        }

        // Stream with Schema followed by another Schema instead of RecordBatch -> ReadNextRecordBatch throws InvalidDataException
        var abwTwoSchemas = new ArrayBufferWriter<byte>();
        ArrowStreamWriter.WriteSchema(abwTwoSchemas, schema);
        ArrowStreamWriter.WriteSchema(abwTwoSchemas, schema);
        using (var reader = new ArrowStreamReader(new MemoryStream(abwTwoSchemas.WrittenSpan.ToArray())))
        {
            reader.ReadSchema();
            Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
        }
    }

    private static byte[] BuildSyntheticIpcBatchFrame(
        ArrowSchema schema,
        int rowCount,
        ArrowColumnDescriptor[] descriptors,
        byte[] bodyBytes)
    {
        var abw = new ArrayBufferWriter<byte>();
        ArrowStreamWriter.WriteSchema(abw, schema);

        int payloadLen = 8 + descriptors.Length * ArrowColumnDescriptor.Size;
        int metaLen = 16 + payloadLen;
        int metaPad = (8 - (metaLen % 8)) % 8;

        Span<byte> frame = abw.GetSpan(8 + metaLen + metaPad);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(frame.Slice(4, 4), metaLen);
        frame[8] = (byte)ArrowMessageType.RecordBatch;
        frame[9] = 0;
        frame[10] = 0;
        frame[11] = 0;
        BinaryPrimitives.WriteInt64LittleEndian(frame.Slice(12, 8), bodyBytes.Length);
        BinaryPrimitives.WriteInt32LittleEndian(frame.Slice(20, 4), payloadLen);
        BinaryPrimitives.WriteInt32LittleEndian(frame.Slice(24, 4), rowCount);
        BinaryPrimitives.WriteInt32LittleEndian(frame.Slice(28, 4), descriptors.Length);
        for (int i = 0; i < descriptors.Length; i++)
        {
            descriptors[i].WriteTo(frame.Slice(32 + i * ArrowColumnDescriptor.Size, ArrowColumnDescriptor.Size));
        }
        if (metaPad > 0)
        {
            frame.Slice(8 + metaLen, metaPad).Clear();
        }
        abw.Advance(8 + metaLen + metaPad);

        if (bodyBytes.Length > 0)
        {
            Span<byte> dst = abw.GetSpan(bodyBytes.Length);
            bodyBytes.CopyTo(dst);
            abw.Advance(bodyBytes.Length);
        }

        ArrowStreamWriter.WriteEndOfStream(abw);
        return abw.WrittenSpan.ToArray();
    }

    private static void AssertBatchRejectedBothPaths(byte[] wireBytes)
    {
        // Stream path
        using (var reader = new ArrowStreamReader(new MemoryStream(wireBytes)))
        {
            reader.ReadSchema();
            Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
        }

        // Zero-copy ReadOnlyMemory path
        var schema = ArrowStreamReader.ReadSchema(wireBytes, out int consumed);
        Assert.Throws<InvalidDataException>(() =>
            ArrowStreamReader.ReadRecordBatch(wireBytes.AsMemory(consumed), schema, out _));
    }

    private static ArrowRecordBatch MakeNullableBatch(ArrowSchema schema, int?[] ids, bool?[] flags, string?[] tags)
    {
        int rows = ids.Length;
        byte[] idNulls = new byte[(rows + 7) >> 3];
        int idNullCount = 0;
        int[] idVals = new int[rows];

        byte[] flagNulls = new byte[(rows + 7) >> 3];
        int flagNullCount = 0;
        byte[] flagVals = new byte[(rows + 7) >> 3];

        byte[] tagNulls = new byte[(rows + 7) >> 3];
        int tagNullCount = 0;
        int[] tagOffsets = new int[rows + 1];
        using var tagMs = new MemoryStream();

        for (int i = 0; i < rows; i++)
        {
            if (ids[i] is int v)
            {
                idVals[i] = v;
                idNulls[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                idNullCount++;
            }

            if (flags[i] is bool b)
            {
                flagNulls[i >> 3] |= (byte)(1 << (i & 7));
                if (b) flagVals[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                flagNullCount++;
            }

            tagOffsets[i] = (int)tagMs.Position;
            if (tags[i] is string s)
            {
                tagNulls[i >> 3] |= (byte)(1 << (i & 7));
                tagMs.Write(Encoding.UTF8.GetBytes(s));
            }
            else
            {
                tagNullCount++;
            }
        }
        tagOffsets[rows] = (int)tagMs.Position;

        return new ArrowRecordBatch(schema, rows, [
            new ArrowColumn(schema.GetField(0), rows, idNullCount, idNullCount > 0 ? idNulls : ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(idVals.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(1), rows, flagNullCount, flagNullCount > 0 ? flagNulls : ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, flagVals),
            new ArrowColumn(schema.GetField(2), rows, tagNullCount, tagNullCount > 0 ? tagNulls : ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(tagOffsets.AsSpan()).ToArray(), tagMs.ToArray())
        ]);
    }

    private sealed class NonSeekableStreamWrapper(Stream inner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => inner.Read(buffer);
        public override int ReadByte() => inner.ReadByte();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
