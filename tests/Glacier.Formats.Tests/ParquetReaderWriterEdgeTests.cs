namespace Glacier.Formats.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Glacier.Formats.Arrow;
using Glacier.Formats.Parquet;
using Xunit;

public class ParquetReaderWriterEdgeTests
{
    private static ArrowRecordBatch CreateSampleBatch(int rowCount, int idStart, bool boolPattern)
    {
        var schema = new ArrowSchema([
            new ArrowField("flag", ArrowType.Boolean, isNullable: false),
            new ArrowField("i32", ArrowType.Int32, isNullable: false),
            new ArrowField("i64", ArrowType.Int64, isNullable: false),
            new ArrowField("f32", ArrowType.Float, isNullable: false),
            new ArrowField("f64", ArrowType.Double, isNullable: false),
            new ArrowField("text", ArrowType.Utf8, isNullable: true)
        ]);

        byte[] bools = new byte[(rowCount + 7) / 8];
        var i32s = new int[rowCount];
        var i64s = new long[rowCount];
        var f32s = new float[rowCount];
        var f64s = new double[rowCount];
        var offsets = new int[rowCount + 1];
        using var strMs = new MemoryStream();

        for (int i = 0; i < rowCount; i++)
        {
            if ((i % 2 == 0) == boolPattern)
            {
                bools[i >> 3] |= (byte)(1 << (i & 7));
            }
            i32s[i] = idStart + i;
            i64s[i] = 10_000_000_000L + idStart + i;
            f32s[i] = (idStart + i) * 0.5f;
            f64s[i] = (idStart + i) * 1.25;
            offsets[i] = (int)strMs.Position;
            strMs.Write(Encoding.UTF8.GetBytes($"row_{idStart + i}"));
        }
        offsets[rowCount] = (int)strMs.Position;

        return new ArrowRecordBatch(schema, rowCount, [
            new ArrowColumn(schema.GetField(0), rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, bools),
            new ArrowColumn(schema.GetField(1), rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(i32s.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(2), rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(i64s.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(3), rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(f32s.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(4), rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(f64s.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(5), rowCount, 0, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray(), strMs.ToArray())
        ]);
    }

    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    public void ParquetWriterReader_AllTypesSingleRowGroup_Roundtrips(CompressionCodec codec)
    {
        var batch = CreateSampleBatch(25, idStart: 0, boolPattern: true);
        byte[] bytes = ParquetWriter.WriteToBytes(batch, codec);
        var read = ParquetReader.ReadBytes(bytes);

        Assert.Equal(25, read.RowCount);
        Assert.Equal(6, read.ColumnCount);
        for (int i = 0; i < 25; i++)
        {
            Assert.Equal(i % 2 == 0, read.Column(0).GetBoolean(i));
            Assert.Equal(i, read.Column(1).GetInt32(i));
            Assert.Equal(10_000_000_000L + i, read.Column(2).GetInt64(i));
            Assert.Equal(i * 0.5f, read.Column(3).GetFloat(i), 4);
            Assert.Equal(i * 1.25, read.Column(4).GetDouble(i), 4);
            Assert.Equal($"row_{i}", read.Column(5).GetString(i));
        }
    }

    [Fact]
    public void ParquetWriterReader_MultiRowGroup_CombinesAllBatches()
    {
        var b1 = CreateSampleBatch(10, idStart: 0, boolPattern: true);
        var b2 = CreateSampleBatch(15, idStart: 10, boolPattern: false);
        var b3 = CreateSampleBatch(5, idStart: 25, boolPattern: true);

        using var ms = new MemoryStream();
        ParquetWriter.WriteStream(ms, [b1, b2, b3], CompressionCodec.Snappy);
        ms.Seek(0, SeekOrigin.Begin);

        var read = ParquetReader.ReadStream(ms);
        Assert.Equal(30, read.RowCount);
        Assert.Equal(6, read.ColumnCount);

        Assert.True(read.Column(0).GetBoolean(0));
        Assert.False(read.Column(0).GetBoolean(10));
        Assert.True(read.Column(0).GetBoolean(11));
        Assert.True(read.Column(0).GetBoolean(25));

        for (int i = 0; i < 30; i++)
        {
            Assert.Equal(i, read.Column(1).GetInt32(i));
            Assert.Equal(10_000_000_000L + i, read.Column(2).GetInt64(i));
            Assert.Equal(i * 0.5f, read.Column(3).GetFloat(i), 4);
            Assert.Equal(i * 1.25, read.Column(4).GetDouble(i), 4);
            Assert.Equal($"row_{i}", read.Column(5).GetString(i));
        }
    }

    [Fact]
    public void ParquetWriterReader_EmptyBatchesAndFileRoundtrip()
    {
        using var emptyMs = new MemoryStream();
        ParquetWriter.WriteStream(emptyMs, Array.Empty<ArrowRecordBatch>());
        emptyMs.Seek(0, SeekOrigin.Begin);
        var emptyRead = ParquetReader.ReadStream(emptyMs);
        Assert.Equal(0, emptyRead.RowCount);
        Assert.Empty(emptyRead.Columns);

        string tempPath = Path.Combine(Path.GetTempPath(), $"glacier_fmt_{Guid.NewGuid():N}.parquet");
        try
        {
            var batch = CreateSampleBatch(7, idStart: 100, boolPattern: true);
            ParquetWriter.WriteFile(tempPath, batch, CompressionCodec.Uncompressed);
            var fileRead = ParquetReader.ReadFile(tempPath);
            Assert.Equal(7, fileRead.RowCount);
            Assert.Equal("row_106", fileRead.Column(5).GetString(6));
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }

    [Fact]
    public void ParquetWriter_InvalidArguments_ThrowExpectedExceptions()
    {
        Assert.Throws<ArgumentNullException>(() => ParquetWriter.WriteToBytes(null!));
        Assert.Throws<ArgumentNullException>(() => ParquetWriter.WriteStream(new MemoryStream(), (ArrowRecordBatch)null!));
        Assert.Throws<ArgumentNullException>(() => ParquetWriter.WriteStream(null!, Array.Empty<ArrowRecordBatch>()));
        Assert.Throws<ArgumentNullException>(() => ParquetWriter.WriteStream(new MemoryStream(), (IReadOnlyList<ArrowRecordBatch>)null!));

        var s1 = new ArrowSchema([new ArrowField("a", ArrowType.Int32)]);
        var s2 = new ArrowSchema([new ArrowField("a", ArrowType.Int32), new ArrowField("b", ArrowType.Int32)]);
        var b1 = new ArrowRecordBatch(s1, 1, [new ArrowColumn(s1.GetField(0), 1, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, new byte[4])]);
        var b2 = new ArrowRecordBatch(s2, 1, [
            new ArrowColumn(s2.GetField(0), 1, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, new byte[4]),
            new ArrowColumn(s2.GetField(1), 1, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, new byte[4])
        ]);

        Assert.Throws<ArgumentException>(() => ParquetWriter.WriteStream(new MemoryStream(), [b1, b2]));
    }

    [Fact]
    public void ParquetReader_InvalidFilesAndUnsupportedCodec_ThrowExpectedExceptions()
    {
        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(new byte[8]));

        byte[] badHeader = "NOPE1234PAR1"u8.ToArray();
        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(badHeader));

        byte[] badFooter = "PAR11234NOPE"u8.ToArray();
        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(badFooter));

        byte[] badMetaLen = "PAR1\xFF\x00\x00\x00PAR1"u8.ToArray();
        Assert.Throws<InvalidDataException>(() => ParquetReader.ReadBytes(badMetaLen));

        // Unsupported codec (e.g., Gzip)
        var batch = CreateSampleBatch(3, 0, true);
        byte[] gzipParquet = ParquetWriter.WriteToBytes(batch, CompressionCodec.Gzip);
        Assert.Throws<NotSupportedException>(() => ParquetReader.ReadBytes(gzipParquet));
    }
}
