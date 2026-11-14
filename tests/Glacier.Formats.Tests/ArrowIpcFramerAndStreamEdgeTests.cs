namespace Glacier.Formats.Tests;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Glacier.Formats.Arrow;
using Xunit;

public class ArrowIpcFramerAndStreamEdgeTests
{
    [Fact]
    public void ArrowIpcParser_TryReadPrefix_HandlesShortContinuationDirectAndEos()
    {
        Assert.False(ArrowIpcParser.TryReadPrefix(new byte[7], out _));

        Span<byte> cont = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(cont[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(cont.Slice(4, 4), 128);
        Assert.True(ArrowIpcParser.TryReadPrefix(cont, out int metaLen));
        Assert.Equal(128, metaLen);

        Span<byte> direct = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(direct[..4], 64);
        Assert.True(ArrowIpcParser.TryReadPrefix(direct, out int directLen));
        Assert.Equal(64, directLen);

        Span<byte> eos = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(eos[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(eos.Slice(4, 4), 0);
        Assert.True(ArrowIpcParser.TryReadPrefix(eos, out int eosLen));
        Assert.Equal(0, eosLen);
    }

    [Fact]
    public void ArrowIpcParser_TryReadMessageHeader_ParsesAndValidatesBounds()
    {
        Assert.False(ArrowIpcParser.TryReadMessageHeader(new byte[4], out _, out _, out _, out _));

        // EOS
        Span<byte> eos = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(eos[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(eos.Slice(4, 4), 0);
        Assert.True(ArrowIpcParser.TryReadMessageHeader(eos, out int eosMeta, out var eosType, out long eosBody, out int eosConsumed));
        Assert.Equal(0, eosMeta);
        Assert.Equal(ArrowMessageType.None, eosType);
        Assert.Equal(0, eosBody);
        Assert.Equal(8, eosConsumed);

        // Truncated metadata
        Span<byte> trunc = stackalloc byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(trunc[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(trunc.Slice(4, 4), 16);
        Assert.False(ArrowIpcParser.TryReadMessageHeader(trunc, out _, out _, out _, out _));

        // Full header with unaligned metadataLength = 19 -> aligned to 24, consumed = 32
        byte[] full = new byte[8 + 19];
        BinaryPrimitives.WriteUInt32LittleEndian(full.AsSpan(0, 4), ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(full.AsSpan(4, 4), 19);
        full[8] = (byte)ArrowMessageType.RecordBatch;
        BinaryPrimitives.WriteInt64LittleEndian(full.AsSpan(12, 8), 4096);

        Assert.True(ArrowIpcParser.TryReadMessageHeader(full, out int mLen, out var mType, out long bLen, out int consumed));
        Assert.Equal(19, mLen);
        Assert.Equal(ArrowMessageType.RecordBatch, mType);
        Assert.Equal(4096, bLen);
        Assert.Equal(32, consumed);
    }

    [Fact]
    public void ArrowStream_ConstructorAndDisposeSemantics()
    {
        Assert.Throws<ArgumentNullException>(() => new ArrowStreamWriter(null!));
        Assert.Throws<ArgumentNullException>(() => new ArrowStreamReader(null!));

        var msOpen = new MemoryStream();
        using (var w = new ArrowStreamWriter(msOpen, leaveOpen: true))
        {
            w.WriteEndOfStream();
        }
        Assert.True(msOpen.CanRead);

        var msClosed = new MemoryStream();
        var writer = new ArrowStreamWriter(msClosed, leaveOpen: false);
        writer.Dispose();
        Assert.False(msClosed.CanWrite);
        Assert.Throws<ObjectDisposedException>(() => writer.WriteEndOfStream());
        Assert.Throws<ObjectDisposedException>(() => writer.WriteSchema(new ArrowSchema([])));

        var readerStream = new MemoryStream([1, 2, 3]);
        var reader = new ArrowStreamReader(readerStream, leaveOpen: false);
        reader.Dispose();
        Assert.False(readerStream.CanRead);
        Assert.Throws<ObjectDisposedException>(() => reader.ReadSchema());
        Assert.Throws<ObjectDisposedException>(() => reader.ReadNextRecordBatch());
    }

    [Fact]
    public void ArrowStream_Roundtrip_NullablesBooleansFloatsAndMultipleBatches()
    {
        var schema = new ArrowSchema([
            new ArrowField("flag", ArrowType.Boolean, isNullable: false),
            new ArrowField("val", ArrowType.Float, isNullable: true)
        ]);

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, leaveOpen: true))
        {
            writer.WriteSchema(schema);

            // Batch 1: 4 rows, row 1 and 3 are null in "val"
            byte[] bools1 = [0b0000_1010]; // true at 1, 3
            float[] floats1 = [1.5f, 0f, 3.5f, 0f];
            byte[] nullBitmap1 = [0b0000_0101]; // valid at 0, 2; null at 1, 3
            var b1 = new ArrowRecordBatch(schema, 4, [
                new ArrowColumn(schema.GetField(0), 4, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, bools1),
                new ArrowColumn(schema.GetField(1), 4, 2, nullBitmap1, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(floats1.AsSpan()).ToArray())
            ]);
            writer.WriteRecordBatch(b1);

            // Batch 2: 2 rows, no nulls
            byte[] bools2 = [0b0000_0001];
            float[] floats2 = [9.25f, -4.5f];
            var b2 = new ArrowRecordBatch(schema, 2, [
                new ArrowColumn(schema.GetField(0), 2, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, bools2),
                new ArrowColumn(schema.GetField(1), 2, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(floats2.AsSpan()).ToArray())
            ]);
            writer.WriteRecordBatch(b2);
            writer.WriteEndOfStream();
        }

        ms.Seek(0, SeekOrigin.Begin);
        using var reader = new ArrowStreamReader(ms);
        Assert.True(reader.HasMoreBatches);

        // ReadNextRecordBatch auto-reads schema if ReadSchema wasn't called explicitly
        var rb1 = reader.ReadNextRecordBatch();
        Assert.NotNull(rb1);
        Assert.NotNull(reader.Schema);
        Assert.Same(reader.Schema, reader.ReadSchema()); // cached

        Assert.Equal(4, rb1.RowCount);
        Assert.False(rb1.Column(0).GetBoolean(0));
        Assert.True(rb1.Column(0).GetBoolean(1));
        Assert.False(rb1.Column(1).IsNull(0));
        Assert.True(rb1.Column(1).IsNull(1));
        Assert.Equal(3.5f, rb1.Column(1).GetFloat(2), 4);
        Assert.True(rb1.Column(1).IsNull(3));

        var rb2 = reader.ReadNextRecordBatch();
        Assert.NotNull(rb2);
        Assert.Equal(2, rb2.RowCount);
        Assert.True(rb2.Column(0).GetBoolean(0));
        Assert.False(rb2.Column(0).GetBoolean(1));
        Assert.Equal(-4.5f, rb2.Column(1).GetFloat(1), 4);

        Assert.Null(reader.ReadNextRecordBatch());
        Assert.False(reader.HasMoreBatches);
    }

    [Fact]
    public void ArrowStream_WideSchema_UsesArrayPoolMetadataPath()
    {
        const int colCount = 35; // 8 + 35*36 = 1268 bytes > 1024 stackalloc threshold
        var fields = new List<ArrowField>(colCount);
        var cols = new List<ArrowColumn>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            var f = new ArrowField($"c{i}", ArrowType.Int32, isNullable: false);
            fields.Add(f);
            cols.Add(new ArrowColumn(f, 1, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, BitConverter.GetBytes(i * 100)));
        }

        var schema = new ArrowSchema(fields);
        var batch = new ArrowRecordBatch(schema, 1, cols);

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, leaveOpen: true))
        {
            writer.WriteSchema(schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEndOfStream();
        }

        ms.Seek(0, SeekOrigin.Begin);
        using var reader = new ArrowStreamReader(ms);
        var readBatch = reader.ReadNextRecordBatch();
        Assert.NotNull(readBatch);
        Assert.Equal(colCount, readBatch.ColumnCount);
        Assert.Equal(3400, readBatch.Column(34).GetInt32(0));
    }

    [Fact]
    public void ArrowStreamReader_InvalidStreams_ThrowExpectedExceptions()
    {
        // Empty stream -> InvalidDataException
        using (var emptyReader = new ArrowStreamReader(new MemoryStream()))
        {
            Assert.Throws<InvalidDataException>(() => emptyReader.ReadSchema());
        }

        // EOS before schema -> InvalidDataException
        var eosMs = new MemoryStream();
        using (var w = new ArrowStreamWriter(eosMs, leaveOpen: true))
        {
            w.WriteEndOfStream();
        }
        eosMs.Seek(0, SeekOrigin.Begin);
        using (var eosReader = new ArrowStreamReader(eosMs))
        {
            Assert.Throws<InvalidDataException>(() => eosReader.ReadSchema());
        }

        // Truncated stream mid-header -> EndOfStreamException
        using (var truncReader = new ArrowStreamReader(new MemoryStream([1, 2, 3])))
        {
            Assert.Throws<EndOfStreamException>(() => truncReader.ReadSchema());
        }
    }
}
