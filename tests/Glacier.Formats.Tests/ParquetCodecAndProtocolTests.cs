namespace Glacier.Formats.Tests;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Glacier.Formats.Parquet;
using Xunit;

public class ParquetCodecAndProtocolTests
{
    [Fact]
    public void ThriftCompactProtocol_AllTypesLargeDeltaListsAndSkip()
    {
        Assert.Throws<ArgumentNullException>(() => new ThriftCompactWriter(null!));
        Assert.Throws<ArgumentNullException>(() => new ThriftCompactReader(null!));

        using var ms = new MemoryStream();
        var writer = new ThriftCompactWriter(ms);

        writer.WriteStructBegin();
        writer.WriteBool(1, true);
        writer.WriteBool(2, false);
        writer.WriteFieldBegin(3, ThriftType.Byte);
        writer.WriteByte(0xAB);
        writer.WriteFieldBegin(4, ThriftType.I16);
        writer.WriteI16(-12345);
        writer.WriteFieldBegin(25, ThriftType.Double); // delta > 15
        writer.WriteDouble(3.141592653589793);
        writer.WriteFieldBegin(26, ThriftType.List);
        writer.WriteListBegin(ThriftType.I32, 20); // >= 15 triggers varint list size
        for (int i = 0; i < 20; i++) writer.WriteI32(i - 10);
        writer.WriteFieldBegin(27, ThriftType.Struct);
        writer.WriteStructBegin();
        writer.WriteFieldBegin(1, ThriftType.Binary);
        writer.WriteBinary([1, 2, 3, 4]);
        writer.WriteStructEnd();
        writer.WriteFieldStop();

        ms.Seek(0, SeekOrigin.Begin);
        var reader = new ThriftCompactReader(ms);
        reader.ReadStructBegin();

        Assert.True(reader.ReadFieldBegin(out short f1, out var t1));
        Assert.Equal(1, f1);
        Assert.Equal(ThriftType.BooleanTrue, t1);

        Assert.True(reader.ReadFieldBegin(out short f2, out var t2));
        Assert.Equal(2, f2);
        Assert.Equal(ThriftType.BooleanFalse, t2);

        Assert.True(reader.ReadFieldBegin(out short f3, out _));
        Assert.Equal(3, f3);
        Assert.Equal((byte)0xAB, reader.ReadByte());

        Assert.True(reader.ReadFieldBegin(out short f4, out _));
        Assert.Equal(4, f4);
        Assert.Equal((short)-12345, reader.ReadI16());

        Assert.True(reader.ReadFieldBegin(out short f25, out _));
        Assert.Equal(25, f25);
        Assert.Equal(3.141592653589793, reader.ReadDouble(), 12);

        // Skip the list and nested struct to exercise Skip()
        Assert.True(reader.ReadFieldBegin(out short f26, out var t26));
        Assert.Equal(26, f26);
        reader.Skip(t26);

        Assert.True(reader.ReadFieldBegin(out short f27, out var t27));
        Assert.Equal(27, f27);
        reader.Skip(t27);

        Assert.False(reader.ReadFieldBegin(out _, out _));

        // Truncated stream throws EndOfStreamException
        var emptyReader = new ThriftCompactReader(new MemoryStream());
        Assert.Throws<EndOfStreamException>(() => emptyReader.ReadByte());
        Assert.Throws<EndOfStreamException>(() => emptyReader.ReadI32());
        Assert.Throws<EndOfStreamException>(() => emptyReader.ReadI64());
        Assert.Throws<EndOfStreamException>(() => emptyReader.ReadDouble());
    }

    [Fact]
    public void SnappyCodec_EdgeCasesAndInvalidInputs()
    {
        Assert.Equal(32, SnappyCodec.GetMaxCompressedLength(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => SnappyCodec.GetMaxCompressedLength(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => SnappyCodec.GetMaxCompressedLength(int.MaxValue));

        // Empty input
        byte[] emptyComp = new byte[4];
        int emptyLen = SnappyCodec.Compress([], emptyComp);
        Assert.Equal(1, emptyLen);
        Assert.Equal(0, emptyComp[0]);
        Assert.Equal(0, SnappyCodec.Decompress(emptyComp.AsSpan(0, emptyLen), []));
        Assert.Equal(0, SnappyCodec.Decompress([], []));

        // Short input (< 16 bytes)
        byte[] shortSrc = [1, 2, 3, 4, 5, 6, 7];
        byte[] shortComp = new byte[SnappyCodec.GetMaxCompressedLength(shortSrc.Length)];
        int shortCompLen = SnappyCodec.Compress(shortSrc, shortComp);
        byte[] shortDec = new byte[shortSrc.Length];
        Assert.Equal(shortSrc.Length, SnappyCodec.Decompress(shortComp.AsSpan(0, shortCompLen), shortDec));
        Assert.Equal(shortSrc, shortDec);

        // Overlapping RLE copy (all identical bytes -> offset < 8)
        byte[] rleSrc = new byte[5000];
        Array.Fill(rleSrc, (byte)'A');
        byte[] rleComp = new byte[SnappyCodec.GetMaxCompressedLength(rleSrc.Length)];
        int rleCompLen = SnappyCodec.Compress(rleSrc, rleComp);
        byte[] rleDec = new byte[rleSrc.Length];
        Assert.Equal(rleSrc.Length, SnappyCodec.Decompress(rleComp.AsSpan(0, rleCompLen), rleDec));
        Assert.Equal(rleSrc, rleDec);

        // Incompressible random data (hits 1-byte and 2-byte extra literal headers)
        byte[] rndSrc = new byte[70_000];
        new Random(42).NextBytes(rndSrc);
        byte[] rndComp = new byte[SnappyCodec.GetMaxCompressedLength(rndSrc.Length)];
        int rndCompLen = SnappyCodec.Compress(rndSrc, rndComp);
        byte[] rndDec = new byte[rndSrc.Length];
        Assert.Equal(rndSrc.Length, SnappyCodec.Decompress(rndComp.AsSpan(0, rndCompLen), rndDec));
        Assert.Equal(rndSrc, rndDec);

        // Buffer too small
        Assert.Throws<ArgumentException>(() => SnappyCodec.Compress([], []));
        Assert.Throws<ArgumentException>(() => SnappyCodec.Compress(shortSrc, new byte[2]));
        Assert.Throws<ArgumentException>(() => SnappyCodec.Decompress(shortComp.AsSpan(0, shortCompLen), new byte[2]));

        // Corrupt Snappy streams
        Assert.Throws<InvalidDataException>(() => SnappyCodec.Decompress([0x80], new byte[16])); // truncated varint
        Assert.Throws<InvalidDataException>(() => SnappyCodec.Decompress([0x80, 0x80, 0x80, 0x80, 0x10], new byte[16])); // varint overflow
        Assert.Throws<InvalidDataException>(() => SnappyCodec.Decompress([0x04, 0x00, 0x01], new byte[4])); // length mismatch (declared 4, produced 1)
        Assert.Throws<InvalidDataException>(() => SnappyCodec.Decompress([0x04, 0x01, 0x01], new byte[4])); // invalid 1-byte copy offset before any literals
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(13)]
    [InlineData(16)]
    [InlineData(20)]
    [InlineData(29)]
    [InlineData(32)]
    public void VectorizedBitUnpacker_NonMultipleOf8AndOddBitWidths_Roundtrips(int bitWidth)
    {
        const int count = 37; // intentionally not a multiple of 8
        uint mask = bitWidth == 32 ? uint.MaxValue : bitWidth == 0 ? 0u : (1u << bitWidth) - 1u;
        var original = new uint[count];
        for (int i = 0; i < count; i++)
        {
            original[i] = bitWidth == 0 ? 0u : (uint)(i * 13 + 5) & mask;
        }

        byte[] packed = new byte[(count * bitWidth + 7) / 8 + 8];
        int bitOffset = 0;
        for (int i = 0; i < count; i++)
        {
            for (int b = 0; b < bitWidth; b++)
            {
                if (((original[i] >> b) & 1) != 0)
                {
                    packed[(bitOffset + b) >> 3] |= (byte)(1 << ((bitOffset + b) & 7));
                }
            }
            bitOffset += bitWidth;
        }

        var unpacked = new uint[count];
        Array.Fill(unpacked, 0xDEADBEEF);
        VectorizedBitUnpacker.Unpack(packed, bitWidth, unpacked);
        Assert.Equal(original, unpacked);
    }

    [Fact]
    public void ParquetRleCodec_EdgeCasesAndRleRuns()
    {
        Assert.Equal(0, ParquetRleCodec.Encode([], 4, new byte[16]));

        var zeros = new uint[10];
        Array.Fill(zeros, 99u);
        Assert.Equal(10, ParquetRleCodec.Decode([], 0, zeros));
        Assert.All(zeros, v => Assert.Equal(0u, v));

        // Large non-multiple-of-8 count (> 512 values -> multiple 64-group chunks)
        const int count = 600;
        var vals = new uint[count];
        for (int i = 0; i < count; i++) vals[i] = (uint)(i % 31);
        byte[] encoded = new byte[2048];
        int encLen = ParquetRleCodec.Encode(vals, 5, encoded);
        var decoded = new uint[count];
        int decCount = ParquetRleCodec.Decode(encoded.AsSpan(0, encLen), 5, decoded);
        Assert.Equal(count, decCount);
        Assert.Equal(vals, decoded);

        // Hand-crafted RLE run: header = (5 << 1) | 0 = 10, value = 0x0123 (bitWidth = 12 -> 2 bytes LE)
        byte[] rleRun = [10, 0x23, 0x01];
        var rleOut = new uint[5];
        Assert.Equal(5, ParquetRleCodec.Decode(rleRun, 12, rleOut));
        Assert.All(rleOut, v => Assert.Equal(0x0123u, v));
    }

    [Fact]
    public void ParquetThriftCodec_PageAndFileMetaData_Roundtrip()
    {
        using var pageMs = new MemoryStream();
        var dictHeader = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = 512,
            CompressedPageSize = 256,
            DictionaryPageHeader = new DictionaryPageHeader
            {
                NumValues = 64,
                Encoding = ParquetEncoding.PlainDictionary
            }
        };
        ParquetThriftCodec.WritePageHeader(new ThriftCompactWriter(pageMs), dictHeader);
        pageMs.Seek(0, SeekOrigin.Begin);
        var readDict = ParquetThriftCodec.ReadPageHeader(new ThriftCompactReader(pageMs));
        Assert.Equal(PageType.DictionaryPage, readDict.Type);
        Assert.Equal(512, readDict.UncompressedPageSize);
        Assert.Equal(256, readDict.CompressedPageSize);
        Assert.NotNull(readDict.DictionaryPageHeader);
        Assert.Equal(64, readDict.DictionaryPageHeader.NumValues);
        Assert.Equal(ParquetEncoding.PlainDictionary, readDict.DictionaryPageHeader.Encoding);

        using var metaMs = new MemoryStream();
        var fileMeta = new FileMetaData
        {
            Version = 2,
            NumRows = 100,
            CreatedBy = "Glacier.Test",
            Schema = [
                new SchemaElement { Name = "root", NumChildren = 1 },
                new SchemaElement { Name = "x", Type = ParquetType.Int32, RepetitionType = FieldRepetitionType.Required }
            ],
            RowGroups = [
                new RowGroup
                {
                    NumRows = 100,
                    TotalByteSize = 400,
                    Columns = [
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = ParquetType.Int32,
                                Encodings = [ParquetEncoding.Plain],
                                PathInSchema = ["x"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 100,
                                TotalUncompressedSize = 400,
                                TotalCompressedSize = 400,
                                DataPageOffset = 4,
                                DictionaryPageOffset = 4
                            }
                        }
                    ]
                }
            ]
        };
        ParquetThriftCodec.WriteFileMetaData(new ThriftCompactWriter(metaMs), fileMeta);
        metaMs.Seek(0, SeekOrigin.Begin);
        var readMeta = ParquetThriftCodec.ReadFileMetaData(new ThriftCompactReader(metaMs));
        Assert.Equal(2, readMeta.Version);
        Assert.Equal(100, readMeta.NumRows);
        Assert.Equal("Glacier.Test", readMeta.CreatedBy);
        Assert.Equal(2, readMeta.Schema.Count);
        Assert.Single(readMeta.RowGroups);
        Assert.Equal(4, readMeta.RowGroups[0].Columns[0].MetaData!.DictionaryPageOffset);
    }

    [Fact]
    public void ThriftCompactProtocol_NestedStructs_PreservesOuterLastFieldIdAcrossWritersReadersAndSkip()
    {
        Assert.Throws<InvalidOperationException>(() => new ThriftCompactWriter(new MemoryStream()).WriteStructEnd());
        Assert.Throws<InvalidOperationException>(() => new ThriftBufferWriter(new ArrayBufferWriter<byte>()).WriteStructEnd());

        static void WriteNestedPayload(Action<Action<ThriftCompactWriter>> writeWithStream, out byte[] streamBytes, out byte[] bufferBytes)
        {
            using var ms = new MemoryStream();
            var sw = new ThriftCompactWriter(ms);
            sw.WriteStructBegin();
            sw.WriteFieldBegin(4, ThriftType.Struct); // outer field 4
            sw.WriteStructBegin();
            sw.WriteFieldBegin(1, ThriftType.I32);
            sw.WriteI32(10);
            sw.WriteFieldBegin(2, ThriftType.I32);
            sw.WriteI32(20);
            sw.WriteFieldBegin(3, ThriftType.I32); // inner ends at field 3 (< outer 4)
            sw.WriteI32(30);
            sw.WriteStructEnd();
            sw.WriteFieldBegin(6, ThriftType.Binary); // outer field 6 (delta 2 from 4, NOT 3 from 3)
            sw.WriteString("outer-after-nested");
            sw.WriteFieldBegin(7, ThriftType.Struct); // deep nesting > 16 to exercise overflow stack
            for (int d = 0; d < 20; d++)
            {
                sw.WriteStructBegin();
                sw.WriteFieldBegin(1, ThriftType.I32);
                sw.WriteI32(d);
                if (d < 19)
                {
                    sw.WriteFieldBegin(2, ThriftType.Struct);
                }
            }
            for (int d = 0; d < 20; d++)
            {
                sw.WriteStructEnd();
            }
            sw.WriteFieldBegin(9, ThriftType.I64);
            sw.WriteI64(987654321L);
            sw.WriteStructEnd();
            streamBytes = ms.ToArray();

            var abw = new ArrayBufferWriter<byte>();
            var bw = new ThriftBufferWriter(abw);
            bw.WriteStructBegin();
            bw.WriteFieldBegin(4, ThriftType.Struct);
            bw.WriteStructBegin();
            bw.WriteFieldBegin(1, ThriftType.I32);
            bw.WriteI32(10);
            bw.WriteFieldBegin(2, ThriftType.I32);
            bw.WriteI32(20);
            bw.WriteFieldBegin(3, ThriftType.I32);
            bw.WriteI32(30);
            bw.WriteStructEnd();
            bw.WriteFieldBegin(6, ThriftType.Binary);
            bw.WriteString("outer-after-nested");
            bw.WriteFieldBegin(7, ThriftType.Struct);
            for (int d = 0; d < 20; d++)
            {
                bw.WriteStructBegin();
                bw.WriteFieldBegin(1, ThriftType.I32);
                bw.WriteI32(d);
                if (d < 19)
                {
                    bw.WriteFieldBegin(2, ThriftType.Struct);
                }
            }
            for (int d = 0; d < 20; d++)
            {
                bw.WriteStructEnd();
            }
            bw.WriteFieldBegin(9, ThriftType.I64);
            bw.WriteI64(987654321L);
            bw.WriteStructEnd();
            bufferBytes = abw.WrittenSpan.ToArray();
            _ = writeWithStream;
        }

        WriteNestedPayload(_ => { }, out byte[] streamBytes, out byte[] bufferBytes);
        Assert.Equal(streamBytes, bufferBytes);

        // 1. Read all fields cleanly with ThriftCompactReader
        var sr = new ThriftCompactReader(new MemoryStream(streamBytes));
        sr.ReadStructBegin();
        Assert.True(sr.ReadFieldBegin(out short f4, out var t4));
        Assert.Equal(4, f4);
        Assert.Equal(ThriftType.Struct, t4);
        sr.ReadStructBegin();
        Assert.True(sr.ReadFieldBegin(out short inner1, out _));
        Assert.Equal(1, inner1);
        Assert.Equal(10, sr.ReadI32());
        Assert.True(sr.ReadFieldBegin(out short inner2, out _));
        Assert.Equal(2, inner2);
        Assert.Equal(20, sr.ReadI32());
        Assert.True(sr.ReadFieldBegin(out short inner3, out _));
        Assert.Equal(3, inner3);
        Assert.Equal(30, sr.ReadI32());
        Assert.False(sr.ReadFieldBegin(out _, out _));

        Assert.True(sr.ReadFieldBegin(out short f6, out _));
        Assert.Equal(6, f6);
        Assert.Equal("outer-after-nested", sr.ReadString());

        Assert.True(sr.ReadFieldBegin(out short f7, out var t7));
        Assert.Equal(7, f7);
        sr.Skip(t7);

        Assert.True(sr.ReadFieldBegin(out short f9, out _));
        Assert.Equal(9, f9);
        Assert.Equal(987654321L, sr.ReadI64());
        Assert.False(sr.ReadFieldBegin(out _, out _));

        // 2. Skip nested struct with ThriftSpanReader and verify outer field IDs remain exact
        var spanReader = new ThriftSpanReader(bufferBytes);
        spanReader.ReadStructBegin();
        Assert.True(spanReader.ReadFieldBegin(out short sf4, out var st4));
        Assert.Equal(4, sf4);
        spanReader.Skip(st4);
        Assert.True(spanReader.ReadFieldBegin(out short sf6, out _));
        Assert.Equal(6, sf6);
        Assert.Equal("outer-after-nested", spanReader.ReadString());
        Assert.True(spanReader.ReadFieldBegin(out short sf7, out var st7));
        Assert.Equal(7, sf7);
        spanReader.Skip(st7);
        Assert.True(spanReader.ReadFieldBegin(out short sf9, out _));
        Assert.Equal(9, sf9);
        Assert.Equal(987654321L, spanReader.ReadI64());
        Assert.False(spanReader.ReadFieldBegin(out _, out _));
    }

    [Fact]
    public void ParquetThriftCodec_WireFieldIds_MatchApacheParquetThriftSpec()
    {
        var meta = new FileMetaData
        {
            Version = 1,
            NumRows = 42,
            CreatedBy = "Glacier.Formats 1.0",
            Schema = [
                new SchemaElement { Name = "schema", NumChildren = 1 },
                new SchemaElement { Name = "id", Type = ParquetType.Int32, RepetitionType = FieldRepetitionType.Required }
            ],
            RowGroups = [
                new RowGroup
                {
                    NumRows = 42,
                    TotalByteSize = 168,
                    Columns = [
                        new ColumnChunk
                        {
                            FileOffset = 4,
                            MetaData = new ColumnMetaData
                            {
                                Type = ParquetType.Int32,
                                Encodings = [ParquetEncoding.Plain],
                                PathInSchema = ["id"],
                                Codec = CompressionCodec.Uncompressed,
                                NumValues = 42,
                                TotalUncompressedSize = 168,
                                TotalCompressedSize = 168,
                                DataPageOffset = 4,
                                DictionaryPageOffset = 12
                            }
                        }
                    ]
                }
            ]
        };

        using var ms = new MemoryStream();
        ParquetThriftCodec.WriteFileMetaData(new ThriftCompactWriter(ms), meta);
        byte[] streamWire = ms.ToArray();

        var abw = new ArrayBufferWriter<byte>();
        ParquetThriftCodec.WriteFileMetaData(abw, meta);
        byte[] bufferWire = abw.WrittenSpan.ToArray();
        Assert.Equal(streamWire, bufferWire);

        // Verify FileMetaData top-level field IDs directly on raw bytes
        var topFieldIds = new List<short>();
        var colMetaFieldIds = new List<short>();
        var r = new ThriftCompactReader(new MemoryStream(streamWire));
        r.ReadStructBegin();
        while (r.ReadFieldBegin(out short fid, out var ft))
        {
            topFieldIds.Add(fid);
            if (fid == 4) // RowGroups
            {
                r.ReadListBegin(out _, out int rgCount);
                Assert.Equal(1, rgCount);
                r.ReadStructBegin();
                while (r.ReadFieldBegin(out short rgFid, out var rgFt))
                {
                    if (rgFid == 1) // Columns
                    {
                        r.ReadListBegin(out _, out int colCount);
                        Assert.Equal(1, colCount);
                        r.ReadStructBegin();
                        while (r.ReadFieldBegin(out short cFid, out var cFt))
                        {
                            if (cFid == 3) // ColumnMetaData
                            {
                                r.ReadStructBegin();
                                while (r.ReadFieldBegin(out short cmFid, out var cmFt))
                                {
                                    colMetaFieldIds.Add(cmFid);
                                    r.Skip(cmFt);
                                }
                            }
                            else
                            {
                                r.Skip(cFt);
                            }
                        }
                    }
                    else
                    {
                        r.Skip(rgFt);
                    }
                }
            }
            else
            {
                r.Skip(ft);
            }
        }

        // FileMetaData: 1=version, 2=schema, 3=num_rows, 4=row_groups, 6=created_by (never 7!)
        Assert.Equal([1, 2, 3, 4, 6], topFieldIds);
        // ColumnMetaData: 1=type, 2=encodings, 3=path_in_schema, 4=codec, 5=num_values,
        // 6=total_uncompressed_size, 7=total_compressed_size, 9=data_page_offset, 11=dictionary_page_offset
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 9, 11], colMetaFieldIds);
    }

    [Fact]
    public void ParquetThriftCodec_ReadsStandardOptionalMetadataFields8And10AndStatistics()
    {
        // Build standard Apache Parquet FileMetaData containing optional field 5 (key_value_metadata),
        // ColumnMetaData field 8 (key_value_metadata), field 9 (data_page_offset),
        // field 10 (index_page_offset), field 11 (dictionary_page_offset), and field 12 (statistics).
        using var metaMs = new MemoryStream();
        var w = new ThriftCompactWriter(metaMs);
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.I32);
        w.WriteI32(1);
        w.WriteFieldBegin(2, ThriftType.List);
        w.WriteListBegin(ThriftType.Struct, 2);
        w.WriteStructBegin();
        w.WriteFieldBegin(4, ThriftType.Binary);
        w.WriteString("schema");
        w.WriteFieldBegin(5, ThriftType.I32);
        w.WriteI32(1);
        w.WriteStructEnd();
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.I32);
        w.WriteI32((int)ParquetType.Int32);
        w.WriteFieldBegin(3, ThriftType.I32);
        w.WriteI32((int)FieldRepetitionType.Required);
        w.WriteFieldBegin(4, ThriftType.Binary);
        w.WriteString("col");
        w.WriteStructEnd();
        w.WriteFieldBegin(3, ThriftType.I64);
        w.WriteI64(5);
        w.WriteFieldBegin(4, ThriftType.List);
        w.WriteListBegin(ThriftType.Struct, 1);
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.List);
        w.WriteListBegin(ThriftType.Struct, 1);
        w.WriteStructBegin();
        w.WriteFieldBegin(2, ThriftType.I64);
        w.WriteI64(4);
        w.WriteFieldBegin(3, ThriftType.Struct);
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.I32);
        w.WriteI32((int)ParquetType.Int32);
        w.WriteFieldBegin(2, ThriftType.List);
        w.WriteListBegin(ThriftType.I32, 1);
        w.WriteI32((int)ParquetEncoding.Plain);
        w.WriteFieldBegin(3, ThriftType.List);
        w.WriteListBegin(ThriftType.Binary, 1);
        w.WriteString("col");
        w.WriteFieldBegin(4, ThriftType.I32);
        w.WriteI32((int)CompressionCodec.Uncompressed);
        w.WriteFieldBegin(5, ThriftType.I64);
        w.WriteI64(5);
        w.WriteFieldBegin(6, ThriftType.I64);
        w.WriteI64(20);
        w.WriteFieldBegin(7, ThriftType.I64);
        w.WriteI64(20);
        w.WriteFieldBegin(8, ThriftType.List); // ColumnMetaData.key_value_metadata
        w.WriteListBegin(ThriftType.Struct, 1);
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.Binary);
        w.WriteString("k");
        w.WriteFieldBegin(2, ThriftType.Binary);
        w.WriteString("v");
        w.WriteStructEnd();
        w.WriteFieldBegin(9, ThriftType.I64); // ColumnMetaData.data_page_offset
        w.WriteI64(1234);
        w.WriteFieldBegin(10, ThriftType.I64); // ColumnMetaData.index_page_offset
        w.WriteI64(5678);
        w.WriteFieldBegin(11, ThriftType.I64); // ColumnMetaData.dictionary_page_offset
        w.WriteI64(999);
        w.WriteFieldBegin(12, ThriftType.Struct); // ColumnMetaData.statistics
        w.WriteStructBegin();
        w.WriteFieldBegin(3, ThriftType.I64);
        w.WriteI64(0);
        w.WriteStructEnd();
        w.WriteStructEnd(); // ColumnMetaData
        w.WriteStructEnd(); // ColumnChunk
        w.WriteFieldBegin(2, ThriftType.I64);
        w.WriteI64(20);
        w.WriteFieldBegin(3, ThriftType.I64);
        w.WriteI64(5);
        w.WriteStructEnd(); // RowGroup
        w.WriteFieldBegin(5, ThriftType.List); // FileMetaData.key_value_metadata
        w.WriteListBegin(ThriftType.Struct, 1);
        w.WriteStructBegin();
        w.WriteFieldBegin(1, ThriftType.Binary);
        w.WriteString("writer");
        w.WriteFieldBegin(2, ThriftType.Binary);
        w.WriteString("apache");
        w.WriteStructEnd();
        w.WriteFieldBegin(6, ThriftType.Binary); // FileMetaData.created_by
        w.WriteString("parquet-mr");
        w.WriteStructEnd();

        byte[] raw = metaMs.ToArray();
        FileMetaData fromStream = ParquetThriftCodec.ReadFileMetaData(new ThriftCompactReader(new MemoryStream(raw)));
        FileMetaData fromSpan = ParquetThriftCodec.ReadFileMetaData(raw);

        foreach (var decoded in new[] { fromStream, fromSpan })
        {
            Assert.Equal("parquet-mr", decoded.CreatedBy);
            Assert.Equal(5, decoded.NumRows);
            var cm = decoded.RowGroups[0].Columns[0].MetaData!;
            Assert.Equal(1234, cm.DataPageOffset);
            Assert.Equal(999, cm.DictionaryPageOffset);
        }
    }
}
