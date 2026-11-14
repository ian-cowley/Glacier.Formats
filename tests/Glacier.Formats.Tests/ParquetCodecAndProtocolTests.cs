namespace Glacier.Formats.Tests;

using System;
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
}
