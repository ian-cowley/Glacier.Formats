namespace Glacier.Formats.Arrow;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

/// <summary>
/// In-memory columnar buffer set for a single column of an <see cref="ArrowRecordBatch"/>.
/// </summary>
public sealed class ArrowColumn
{
    /// <summary>Gets the schema field describing this column.</summary>
    public ArrowField Field { get; }
    /// <summary>Gets the logical row count in this column.</summary>
    public int Length { get; }
    /// <summary>Gets the number of null elements in this column.</summary>
    public int NullCount { get; }

    /// <summary>Gets the LSB-first validity bitmap (<c>1</c> = valid, <c>0</c> = null), or empty when <see cref="NullCount"/> is 0.</summary>
    public ReadOnlyMemory<byte> NullBitmap { get; }
    /// <summary>Gets the 32-bit little-endian byte offsets buffer for variable-length (<see cref="ArrowTypeId.Utf8"/> / <see cref="ArrowTypeId.Binary"/>) columns.</summary>
    public ReadOnlyMemory<byte> OffsetsBuffer { get; }
    /// <summary>Gets the primary column value buffer.</summary>
    public ReadOnlyMemory<byte> DataBuffer { get; }

    /// <summary>
    /// Initializes a new <see cref="ArrowColumn"/>.
    /// </summary>
    /// <param name="field">The column field definition.</param>
    /// <param name="length">Logical row count.</param>
    /// <param name="nullCount">Null value count.</param>
    /// <param name="nullBitmap">Validity bitmap bytes.</param>
    /// <param name="offsetsBuffer">Offsets buffer bytes for variable-length columns.</param>
    /// <param name="dataBuffer">Primary data buffer bytes.</param>
    public ArrowColumn(
        ArrowField field,
        int length,
        int nullCount,
        ReadOnlyMemory<byte> nullBitmap,
        ReadOnlyMemory<byte> offsetsBuffer,
        ReadOnlyMemory<byte> dataBuffer)
    {
        Field = field ?? throw new ArgumentNullException(nameof(field));
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), length, "Column length cannot be negative.");
        if (nullCount < 0 || nullCount > length)
            throw new ArgumentOutOfRangeException(nameof(nullCount), nullCount, $"NullCount must be between 0 and Length ({length}).");

        Length = length;
        NullCount = nullCount;
        NullBitmap = nullBitmap;
        OffsetsBuffer = offsetsBuffer;
        DataBuffer = dataBuffer;
    }

    /// <summary>
    /// Returns <c>true</c> if the value at <paramref name="index"/> is null.
    /// </summary>
    /// <param name="index">Zero-based row index.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsNull(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        if (NullCount == 0 || NullBitmap.IsEmpty) return false;
        int byteIdx = index >> 3;
        int bitIdx = index & 7;
        return (NullBitmap.Span[byteIdx] & (1 << bitIdx)) == 0;
    }

    /// <summary>
    /// Reinterprets the column's <see cref="DataBuffer"/> as a read-only span of <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">Unmanaged element value type.</typeparam>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<T> AsSpan<T>() where T : unmanaged
    {
        int byteCount = checked(Length * Unsafe.SizeOf<T>());
        return MemoryMarshal.Cast<byte, T>(DataBuffer.Span[..byteCount]);
    }

    /// <summary>Reads a 32-bit signed integer at row <paramref name="index"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetInt32(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        ReadOnlySpan<byte> span = DataBuffer.Span.Slice(checked(index * sizeof(int)), sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(span);
    }

    /// <summary>Reads a 64-bit signed integer at row <paramref name="index"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetInt64(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        ReadOnlySpan<byte> span = DataBuffer.Span.Slice(checked(index * sizeof(long)), sizeof(long));
        return BinaryPrimitives.ReadInt64LittleEndian(span);
    }

    /// <summary>Reads a 32-bit IEEE float at row <paramref name="index"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float GetFloat(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        ReadOnlySpan<byte> span = DataBuffer.Span.Slice(checked(index * sizeof(float)), sizeof(float));
        return BinaryPrimitives.ReadSingleLittleEndian(span);
    }

    /// <summary>Reads a 64-bit IEEE double at row <paramref name="index"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double GetDouble(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        ReadOnlySpan<byte> span = DataBuffer.Span.Slice(checked(index * sizeof(double)), sizeof(double));
        return BinaryPrimitives.ReadDoubleLittleEndian(span);
    }

    /// <summary>Reads a bit-packed boolean value at row <paramref name="index"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool GetBoolean(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        int byteIdx = index >> 3;
        int bitIdx = index & 7;
        return (DataBuffer.Span[byteIdx] & (1 << bitIdx)) != 0;
    }

    /// <summary>Decodes the UTF-8 string at row <paramref name="index"/>.</summary>
    public string GetString(int index)
    {
        var bytes = GetBytes(index);
        return bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Returns the raw byte span for the variable-length entry at row <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetBytes(int index)
    {
        if ((uint)index >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(index));
        if (OffsetsBuffer.IsEmpty) return ReadOnlySpan<byte>.Empty;

        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(OffsetsBuffer.Span);
        if ((uint)(index + 1) >= (uint)offsets.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "OffsetsBuffer is shorter than column length.");
        }

        int start = offsets[index];
        int end = offsets[index + 1];
        int len = checked(end - start);
        if (start < 0 || len < 0 || end > DataBuffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index), $"Invalid variable-length offset range [{start}, {end}] for DataBuffer length {DataBuffer.Length}.");
        }

        return DataBuffer.Span.Slice(start, len);
    }

    /// <summary>
    /// Returns a sliced view of this column starting at <paramref name="offset"/> with <paramref name="length"/> rows.
    /// Performs zero-copy slicing whenever byte alignment permits.
    /// </summary>
    /// <param name="offset">Starting row index.</param>
    /// <param name="length">Number of rows to include.</param>
    /// <returns>The sliced <see cref="ArrowColumn"/>.</returns>
    public ArrowColumn Slice(int offset, int length)
    {
        if ((uint)offset > (uint)Length || (uint)length > (uint)(Length - offset))
            throw new ArgumentOutOfRangeException(nameof(offset));

        if (offset == 0 && length == Length)
        {
            return this;
        }

        ReadOnlyMemory<byte> slicedNullBitmap = ReadOnlyMemory<byte>.Empty;
        int slicedNullCount = 0;
        if (NullCount > 0 && !NullBitmap.IsEmpty)
        {
            if ((offset & 7) == 0)
            {
                int byteStart = offset >> 3;
                int byteLen = (length + 7) >> 3;
                ReadOnlyMemory<byte> slice = NullBitmap.Slice(byteStart, byteLen);
                int ones = CountSetBits(slice.Span, length);
                slicedNullCount = length - ones;
                if (slicedNullCount > 0)
                {
                    slicedNullBitmap = slice;
                }
            }
            else
            {
                byte[] sliced = SliceBitmap(NullBitmap.Span, offset, length, out slicedNullCount);
                if (slicedNullCount > 0)
                {
                    slicedNullBitmap = sliced;
                }
            }
        }

        if (Field.DataType.Id == ArrowTypeId.Utf8 || Field.DataType.Id == ArrowTypeId.Binary)
        {
            if (OffsetsBuffer.IsEmpty)
            {
                return new ArrowColumn(Field, length, slicedNullCount, slicedNullBitmap, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
            }

            ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(OffsetsBuffer.Span);
            if (offset + length >= offsets.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(length), "OffsetsBuffer is shorter than slice range.");
            }

            int dataStart = offsets[offset];
            int dataEnd = offsets[offset + length];
            int dataLen = checked(dataEnd - dataStart);
            if (dataStart < 0 || dataLen < 0 || dataEnd > DataBuffer.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), "Invalid variable-length offset range in slice.");
            }

            ReadOnlyMemory<byte> slicedOffsets;
            if (dataStart == 0)
            {
                slicedOffsets = OffsetsBuffer.Slice(0, checked((length + 1) * sizeof(int)));
            }
            else
            {
                byte[] newOffsetsBytes = GC.AllocateUninitializedArray<byte>(checked((length + 1) * sizeof(int)));
                Span<int> newOffsets = MemoryMarshal.Cast<byte, int>(newOffsetsBytes.AsSpan());
                for (int i = 0; i <= length; i++)
                {
                    newOffsets[i] = offsets[offset + i] - dataStart;
                }
                slicedOffsets = newOffsetsBytes;
            }

            ReadOnlyMemory<byte> slicedData = DataBuffer.Slice(dataStart, dataLen);
            return new ArrowColumn(Field, length, slicedNullCount, slicedNullBitmap, slicedOffsets, slicedData);
        }

        if (Field.DataType.Id == ArrowTypeId.Boolean)
        {
            ReadOnlyMemory<byte> slicedBits = (offset & 7) == 0
                ? DataBuffer.Slice(offset >> 3, (length + 7) >> 3)
                : SliceBitmap(DataBuffer.Span, offset, length, out _);
            return new ArrowColumn(Field, length, slicedNullCount, slicedNullBitmap, ReadOnlyMemory<byte>.Empty, slicedBits);
        }

        int elemSize = Field.DataType.ByteWidth;
        if (elemSize == 0)
        {
            return new ArrowColumn(Field, length, slicedNullCount, slicedNullBitmap, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        }

        int byteStartFixed = checked(offset * elemSize);
        int byteLenFixed = checked(length * elemSize);
        ReadOnlyMemory<byte> slicedFixedData = DataBuffer.Slice(byteStartFixed, byteLenFixed);

        return new ArrowColumn(Field, length, slicedNullCount, slicedNullBitmap, ReadOnlyMemory<byte>.Empty, slicedFixedData);
    }

    private static byte[] SliceBitmap(ReadOnlySpan<byte> src, int bitOffset, int bitLength, out int zeroBitCount)
    {
        if (bitLength == 0)
        {
            zeroBitCount = 0;
            return [];
        }

        int dstBytes = (bitLength + 7) >> 3;
        byte[] dst = GC.AllocateUninitializedArray<byte>(dstBytes);
        int srcByte = bitOffset >> 3;
        int bitInByte = bitOffset & 7;

        if (bitInByte == 0)
        {
            src.Slice(srcByte, dstBytes).CopyTo(dst);
        }
        else
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(src);
            ref byte dstRef = ref MemoryMarshal.GetReference(dst.AsSpan());
            int d = 0;

            while (d <= dstBytes - 8 && srcByte + d + 8 < src.Length)
            {
                ulong lo = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref srcRef, srcByte + d));
                ulong hi = Unsafe.Add(ref srcRef, srcByte + d + 8);
                ulong word = (lo >> bitInByte) | (hi << (64 - bitInByte));
                Unsafe.WriteUnaligned(ref Unsafe.Add(ref dstRef, d), word);
                d += 8;
            }

            for (; d < dstBytes; d++)
            {
                byte b0 = src[srcByte + d];
                byte b1 = (srcByte + d + 1 < src.Length) ? src[srcByte + d + 1] : (byte)0;
                dst[d] = (byte)((b0 >> bitInByte) | (b1 << (8 - bitInByte)));
            }
        }

        int remBits = bitLength & 7;
        if (remBits != 0)
        {
            dst[dstBytes - 1] &= (byte)((1 << remBits) - 1);
        }

        int ones = CountSetBits(dst, bitLength);
        zeroBitCount = bitLength - ones;
        return dst;
    }

    private static int CountSetBits(ReadOnlySpan<byte> bitmap, int bitLength)
    {
        if (bitLength == 0) return 0;

        int fullBytes = bitLength >> 3;
        int remBits = bitLength & 7;
        int ones = 0;
        int i = 0;

        ref byte srcRef = ref MemoryMarshal.GetReference(bitmap);
        while (i <= fullBytes - 8)
        {
            ulong word = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref srcRef, i));
            ones += BitOperations.PopCount(word);
            i += 8;
        }

        for (; i < fullBytes; i++)
        {
            ones += BitOperations.PopCount(bitmap[i]);
        }

        if (remBits != 0)
        {
            byte masked = (byte)(bitmap[fullBytes] & ((1 << remBits) - 1));
            ones += BitOperations.PopCount(masked);
        }

        return ones;
    }
}

/// <summary>
/// Represents a tabular batch of aligned <see cref="ArrowColumn"/> buffers sharing a common <see cref="ArrowSchema"/>.
/// </summary>
public sealed class ArrowRecordBatch
{
    /// <summary>Gets the schema describing all columns in this batch.</summary>
    public ArrowSchema Schema { get; }
    /// <summary>Gets the number of rows in this batch.</summary>
    public int RowCount { get; }
    /// <summary>Gets the number of rows in this batch (alias for <see cref="RowCount"/>).</summary>
    public int Length => RowCount;
    /// <summary>Gets the number of columns in this batch.</summary>
    public int ColumnCount => Columns.Count;
    /// <summary>Gets the ordered columns in this batch.</summary>
    public IReadOnlyList<ArrowColumn> Columns { get; }

    /// <summary>
    /// Initializes a new <see cref="ArrowRecordBatch"/>.
    /// </summary>
    /// <param name="schema">The batch schema.</param>
    /// <param name="rowCount">The row count.</param>
    /// <param name="columns">The column buffers.</param>
    public ArrowRecordBatch(ArrowSchema schema, int rowCount, IEnumerable<ArrowColumn> columns)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        ArgumentNullException.ThrowIfNull(columns);
        if (rowCount < 0) throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount, "RowCount cannot be negative.");

        RowCount = rowCount;
        Columns = columns as IReadOnlyList<ArrowColumn> ?? new List<ArrowColumn>(columns);
    }

    /// <summary>Gets the column at zero-based <paramref name="index"/>.</summary>
    public ArrowColumn Column(int index) => Columns[index];

    /// <summary>Gets the column matching <paramref name="name"/> (case-insensitive).</summary>
    public ArrowColumn Column(string name)
    {
        int idx = Schema.GetFieldIndex(name);
        if (idx < 0) throw new KeyNotFoundException($"Column '{name}' not found in schema.");
        return Columns[idx];
    }

    /// <summary>
    /// Returns a sliced <see cref="ArrowRecordBatch"/> covering rows [<paramref name="offset"/>, <paramref name="offset"/> + <paramref name="length"/>).
    /// </summary>
    /// <param name="offset">Zero-based starting row index.</param>
    /// <param name="length">Number of rows to include.</param>
    /// <returns>The sliced <see cref="ArrowRecordBatch"/>.</returns>
    public ArrowRecordBatch Slice(int offset, int length)
    {
        if ((uint)offset > (uint)RowCount || (uint)length > (uint)(RowCount - offset))
            throw new ArgumentOutOfRangeException(nameof(offset));

        if (offset == 0 && length == RowCount)
        {
            return this;
        }

        var slicedCols = new ArrowColumn[Columns.Count];
        for (int i = 0; i < Columns.Count; i++)
        {
            slicedCols[i] = Columns[i].Slice(offset, length);
        }
        return new ArrowRecordBatch(Schema, length, slicedCols);
    }
}
