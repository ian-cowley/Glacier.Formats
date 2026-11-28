namespace Glacier.Formats.Arrow;

using System;
using System.Buffers.Binary;

/// <summary>
/// Fixed 36-byte column buffer slice descriptor used in Glacier Compact Columnar IPC v1
/// record batch metadata (not Apache Arrow FlatBuffers wire-compatible).
/// </summary>
public readonly struct ArrowColumnDescriptor
{
    /// <summary>Zero-based schema field index.</summary>
    public readonly int FieldIndex;
    /// <summary>Number of logical rows in this column.</summary>
    public readonly int Length;
    /// <summary>Number of null values in this column.</summary>
    public readonly int NullCount;
    /// <summary>Byte offset of the validity bitmap within the batch body.</summary>
    public readonly int NullBitmapOffset;
    /// <summary>Byte length of the validity bitmap within the batch body.</summary>
    public readonly int NullBitmapLength;
    /// <summary>Byte offset of the variable-length offsets buffer within the batch body.</summary>
    public readonly int OffsetsOffset;
    /// <summary>Byte length of the variable-length offsets buffer within the batch body.</summary>
    public readonly int OffsetsLength;
    /// <summary>Byte offset of the primary data buffer within the batch body.</summary>
    public readonly int DataOffset;
    /// <summary>Byte length of the primary data buffer within the batch body.</summary>
    public readonly int DataLength;

    /// <summary>
    /// Initializes a new <see cref="ArrowColumnDescriptor"/>.
    /// </summary>
    /// <param name="fieldIndex">Zero-based schema field index.</param>
    /// <param name="length">Logical row count.</param>
    /// <param name="nullCount">Null value count.</param>
    /// <param name="nullBitmapOffset">Byte offset of validity bitmap in batch body.</param>
    /// <param name="nullBitmapLength">Byte length of validity bitmap.</param>
    /// <param name="offsetsOffset">Byte offset of offsets buffer in batch body.</param>
    /// <param name="offsetsLength">Byte length of offsets buffer.</param>
    /// <param name="dataOffset">Byte offset of data buffer in batch body.</param>
    /// <param name="dataLength">Byte length of data buffer.</param>
    public ArrowColumnDescriptor(
        int fieldIndex,
        int length,
        int nullCount,
        int nullBitmapOffset,
        int nullBitmapLength,
        int offsetsOffset,
        int offsetsLength,
        int dataOffset,
        int dataLength)
    {
        FieldIndex = fieldIndex;
        Length = length;
        NullCount = nullCount;
        NullBitmapOffset = nullBitmapOffset;
        NullBitmapLength = nullBitmapLength;
        OffsetsOffset = offsetsOffset;
        OffsetsLength = offsetsLength;
        DataOffset = dataOffset;
        DataLength = dataLength;
    }

    /// <summary>Serialized byte size of an <see cref="ArrowColumnDescriptor"/> (36 bytes).</summary>
    public const int Size = 36; // 9 * 4 bytes

    /// <summary>
    /// Serializes this descriptor in little-endian layout into <paramref name="destination"/>.
    /// </summary>
    /// <param name="destination">Destination span of at least <see cref="Size"/> bytes.</param>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
        {
            throw new ArgumentException($"Destination span must be at least {Size} bytes.", nameof(destination));
        }

        BinaryPrimitives.WriteInt32LittleEndian(destination[..4], FieldIndex);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4, 4), Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), NullCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), NullBitmapOffset);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), NullBitmapLength);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), OffsetsOffset);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(24, 4), OffsetsLength);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(28, 4), DataOffset);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(32, 4), DataLength);
    }

    /// <summary>
    /// Deserializes an <see cref="ArrowColumnDescriptor"/> from little-endian bytes in <paramref name="source"/>.
    /// </summary>
    /// <param name="source">Source span of at least <see cref="Size"/> bytes.</param>
    /// <returns>The decoded descriptor.</returns>
    public static ArrowColumnDescriptor ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
        {
            throw new ArgumentException($"Source span must be at least {Size} bytes.", nameof(source));
        }

        return new ArrowColumnDescriptor(
            BinaryPrimitives.ReadInt32LittleEndian(source[..4]),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(4, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(8, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(12, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(24, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(28, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(32, 4))
        );
    }
}
