namespace Glacier.Formats.Tests;

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Glacier.Formats.Arrow;
using Xunit;

public class ArrowTypesAndBatchTests
{
    [Theory]
    [InlineData(ArrowTypeId.Null, 0)]
    [InlineData(ArrowTypeId.Boolean, 1)]
    [InlineData(ArrowTypeId.Int8, 1)]
    [InlineData(ArrowTypeId.UInt8, 1)]
    [InlineData(ArrowTypeId.Int16, 2)]
    [InlineData(ArrowTypeId.UInt16, 2)]
    [InlineData(ArrowTypeId.Int32, 4)]
    [InlineData(ArrowTypeId.UInt32, 4)]
    [InlineData(ArrowTypeId.Float, 4)]
    [InlineData(ArrowTypeId.Date32, 4)]
    [InlineData(ArrowTypeId.Int64, 8)]
    [InlineData(ArrowTypeId.UInt64, 8)]
    [InlineData(ArrowTypeId.Double, 8)]
    [InlineData(ArrowTypeId.Date64, 8)]
    [InlineData(ArrowTypeId.Timestamp, 8)]
    [InlineData(ArrowTypeId.Duration, 8)]
    [InlineData(ArrowTypeId.Time64, 8)]
    [InlineData(ArrowTypeId.Decimal128, 16)]
    [InlineData(ArrowTypeId.Utf8, 0)]
    [InlineData(ArrowTypeId.Binary, 0)]
    public void ArrowType_ByteWidth_ReturnsExpected(ArrowTypeId id, int expectedWidth)
    {
        var t = new ArrowType(id);
        Assert.Equal(expectedWidth, t.ByteWidth);
    }

    [Fact]
    public void ArrowField_NullName_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ArrowField(null!, ArrowType.Int32));
    }

    [Fact]
    public void ArrowField_ToString_FormatsNullableAndNonNullable()
    {
        var nullable = new ArrowField("age", ArrowType.Int32, isNullable: true);
        var nonNullable = new ArrowField("id", ArrowType.Int64, isNullable: false);

        Assert.Equal("age: Int32?", nullable.ToString());
        Assert.Equal("id: Int64", nonNullable.ToString());
    }

    [Fact]
    public void ArrowSchema_MetadataAndLookups_WorkCorrectly()
    {
        var meta = new Dictionary<string, string> { ["author"] = "glacier" };
        var schema = new ArrowSchema(
            [new ArrowField("UserId", ArrowType.Int64), new ArrowField("Score", ArrowType.Float)],
            meta);

        Assert.Equal("glacier", schema.Metadata["author"]);
        Assert.Equal("UserId", schema.GetFieldByIndex(0).Name);
        Assert.Equal(0, schema.GetFieldIndex("userid")); // case-insensitive
        Assert.Equal(1, schema.GetFieldIndex("SCORE"));
        Assert.Equal(-1, schema.GetFieldIndex("missing"));
    }

    [Fact]
    public void ArrowBufferAndFieldNode_RecordStructs_StoreValues()
    {
        var buf = new ArrowBuffer(64, 128);
        var node = new ArrowFieldNode(100, 5);

        Assert.Equal(64, buf.Offset);
        Assert.Equal(128, buf.Length);
        Assert.Equal(100, node.Length);
        Assert.Equal(5, node.NullCount);
    }

    [Fact]
    public void ArrowColumnDescriptor_WriteToReadFrom_RoundtripsAllFields()
    {
        Assert.Equal(36, ArrowColumnDescriptor.Size);

        var original = new ArrowColumnDescriptor(
            fieldIndex: 3,
            length: 1024,
            nullCount: 17,
            nullBitmapOffset: 8,
            nullBitmapLength: 128,
            offsetsOffset: 136,
            offsetsLength: 4100,
            dataOffset: 4240,
            dataLength: 8192);

        Span<byte> buf = stackalloc byte[ArrowColumnDescriptor.Size];
        original.WriteTo(buf);
        var decoded = ArrowColumnDescriptor.ReadFrom(buf);

        Assert.Equal(original.FieldIndex, decoded.FieldIndex);
        Assert.Equal(original.Length, decoded.Length);
        Assert.Equal(original.NullCount, decoded.NullCount);
        Assert.Equal(original.NullBitmapOffset, decoded.NullBitmapOffset);
        Assert.Equal(original.NullBitmapLength, decoded.NullBitmapLength);
        Assert.Equal(original.OffsetsOffset, decoded.OffsetsOffset);
        Assert.Equal(original.OffsetsLength, decoded.OffsetsLength);
        Assert.Equal(original.DataOffset, decoded.DataOffset);
        Assert.Equal(original.DataLength, decoded.DataLength);
    }

    [Fact]
    public void ArrowColumn_NullField_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ArrowColumn(null!, 0, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty));
    }

    [Fact]
    public void ArrowColumn_IsNull_HandlesMultiByteBitmapAndNoNulls()
    {
        var field = new ArrowField("v", ArrowType.Int32, isNullable: true);
        // 10 rows: bit=1 means valid, bit=0 means null.
        // Byte 0: 0b10101010 (even indices null: 0,2,4,6)
        // Byte 1: 0b00000010 (index 8 null, index 9 valid)
        byte[] bitmap = [0b1010_1010, 0b0000_0010];
        byte[] data = new byte[10 * 4];

        var colWithNulls = new ArrowColumn(field, 10, 5, bitmap, ReadOnlyMemory<byte>.Empty, data);
        for (int i = 0; i < 10; i++)
        {
            bool expectedNull = i % 2 == 0;
            Assert.Equal(expectedNull, colWithNulls.IsNull(i));
        }

        var colNoNulls = new ArrowColumn(field, 10, 0, bitmap, ReadOnlyMemory<byte>.Empty, data);
        for (int i = 0; i < 10; i++)
        {
            Assert.False(colNoNulls.IsNull(i));
        }
    }

    [Fact]
    public void ArrowColumn_TypedAccessors_FloatBooleanAsSpanAndUtf8()
    {
        var floatField = new ArrowField("f", ArrowType.Float);
        float[] floats = [1.25f, -3.5f, 0f, 42.75f];
        var floatCol = new ArrowColumn(floatField, 4, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty,
            MemoryMarshal.AsBytes(floats.AsSpan()).ToArray());

        Assert.Equal(-3.5f, floatCol.GetFloat(1), 4);
        Assert.Equal(floats, floatCol.AsSpan<float>().ToArray());

        var boolField = new ArrowField("b", ArrowType.Boolean);
        // 10 booleans: indices 0,3,7,8 are true -> byte0 = 0b10001001, byte1 = 0b00000001
        byte[] boolBytes = [0b1000_1001, 0b0000_0001];
        var boolCol = new ArrowColumn(boolField, 10, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, boolBytes);
        Assert.True(boolCol.GetBoolean(0));
        Assert.False(boolCol.GetBoolean(1));
        Assert.True(boolCol.GetBoolean(3));
        Assert.True(boolCol.GetBoolean(7));
        Assert.True(boolCol.GetBoolean(8));
        Assert.False(boolCol.GetBoolean(9));

        var strField = new ArrowField("s", ArrowType.Utf8);
        string[] strings = ["", "héllo", "🚀"];
        byte[] s0 = Encoding.UTF8.GetBytes(strings[0]);
        byte[] s1 = Encoding.UTF8.GetBytes(strings[1]);
        byte[] s2 = Encoding.UTF8.GetBytes(strings[2]);
        int[] offsets = [0, s0.Length, s0.Length + s1.Length, s0.Length + s1.Length + s2.Length];
        byte[] allStrBytes = [.. s0, .. s1, .. s2];
        var strCol = new ArrowColumn(strField, 3, 0, ReadOnlyMemory<byte>.Empty,
            MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray(), allStrBytes);

        Assert.Equal("", strCol.GetString(0));
        Assert.Equal("héllo", strCol.GetString(1));
        Assert.Equal("🚀", strCol.GetString(2));

        var emptyOffsetsCol = new ArrowColumn(strField, 1, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        Assert.True(emptyOffsetsCol.GetBytes(0).IsEmpty);
        Assert.Equal(string.Empty, emptyOffsetsCol.GetString(0));
    }

    [Fact]
    public void ArrowColumn_Slice_Utf8AndInvalidBounds()
    {
        var strField = new ArrowField("s", ArrowType.Utf8);
        string[] values = ["alpha", "beta", "gamma", "delta"];
        var dataList = new List<byte>();
        var offsets = new int[values.Length + 1];
        for (int i = 0; i < values.Length; i++)
        {
            offsets[i] = dataList.Count;
            dataList.AddRange(Encoding.UTF8.GetBytes(values[i]));
        }
        offsets[values.Length] = dataList.Count;

        var col = new ArrowColumn(strField, values.Length, 0, ReadOnlyMemory<byte>.Empty,
            MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray(), dataList.ToArray());

        var sliced = col.Slice(1, 2);
        Assert.Equal(2, sliced.Length);
        Assert.Equal("beta", sliced.GetString(0));
        Assert.Equal("gamma", sliced.GetString(1));

        Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(-1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(0, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(2, 3));
    }

    [Fact]
    public void ArrowRecordBatch_PropertiesAndColumnByName_WorkCorrectly()
    {
        Assert.Throws<ArgumentNullException>(() => new ArrowRecordBatch(null!, 0, []));

        var schema = new ArrowSchema([
            new ArrowField("id", ArrowType.Int32),
            new ArrowField("label", ArrowType.Utf8)
        ]);
        int[] ids = [1, 2, 3];
        byte[] sBytes = Encoding.UTF8.GetBytes("abc");
        int[] offsets = [0, 1, 2, 3];

        var batch = new ArrowRecordBatch(schema, 3, [
            new ArrowColumn(schema.GetField(0), 3, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(ids.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(1), 3, 0, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray(), sBytes)
        ]);

        Assert.Equal(3, batch.Length);
        Assert.Equal(2, batch.ColumnCount);
        Assert.Equal(2, batch.Column("LABEL").GetString(1).Length == 1 ? 2 : -1);
        Assert.Equal("b", batch.Column("LABEL").GetString(1));
        Assert.Throws<KeyNotFoundException>(() => batch.Column("nonexistent"));

        var sliced = batch.Slice(1, 2);
        Assert.Equal(2, sliced.RowCount);
        Assert.Equal(2, sliced.Column("id").GetInt32(0));
        Assert.Equal("c", sliced.Column("label").GetString(1));
    }
}
