namespace Glacier.Formats.Arrow;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Text;

/// <summary>
/// Streaming writer for Glacier Compact Columnar IPC v1
/// (Arrow-inspired columnar buffers with custom 8-byte continuation prefix + 16-byte header + 36-byte descriptor framing;
/// not wire-compatible with Apache Arrow FlatBuffers IPC).
/// Guarantees zero heap allocations on the <see cref="WriteRecordBatch(ArrowRecordBatch)"/> hot path.
/// </summary>
public sealed class ArrowStreamWriter : IDisposable
{
    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new <see cref="ArrowStreamWriter"/> writing to <paramref name="stream"/>.
    /// </summary>
    /// <param name="stream">The writable destination stream.</param>
    /// <param name="leaveOpen"><c>true</c> to leave <paramref name="stream"/> open upon disposal.</param>
    public ArrowStreamWriter(Stream stream, bool leaveOpen = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _leaveOpen = leaveOpen;
    }

    /// <summary>
    /// Writes a Glacier Compact Columnar IPC v1 <see cref="ArrowSchema"/> message to the underlying stream.
    /// </summary>
    /// <param name="schema">The schema to serialize.</param>
    public void WriteSchema(ArrowSchema schema)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        ArgumentNullException.ThrowIfNull(schema);

        int payloadSize = ComputeSchemaPayloadSize(schema);
        byte[]? rented = null;
        Span<byte> payload = payloadSize <= 1024
            ? stackalloc byte[payloadSize]
            : (rented = ArrayPool<byte>.Shared.Rent(payloadSize)).AsSpan(0, payloadSize);

        try
        {
            EncodeSchemaPayload(schema, payload);
            WriteFrame(ArrowMessageType.Schema, payload, ReadOnlySpan<byte>.Empty);
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Writes a Glacier Compact Columnar IPC v1 <see cref="ArrowSchema"/> message into <paramref name="writer"/>.
    /// </summary>
    /// <param name="writer">The destination buffer writer.</param>
    /// <param name="schema">The schema to serialize.</param>
    public static void WriteSchema(IBufferWriter<byte> writer, ArrowSchema schema)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(schema);

        int payloadSize = ComputeSchemaPayloadSize(schema);
        byte[]? rented = null;
        Span<byte> payload = payloadSize <= 1024
            ? stackalloc byte[payloadSize]
            : (rented = ArrayPool<byte>.Shared.Rent(payloadSize)).AsSpan(0, payloadSize);

        try
        {
            EncodeSchemaPayload(schema, payload);
            WriteFrameToBufferWriter(writer, ArrowMessageType.Schema, payload, 0);
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Writes an <see cref="ArrowRecordBatch"/> directly to the underlying stream with zero heap allocation.
    /// </summary>
    /// <param name="batch">The record batch to serialize.</param>
    public void WriteRecordBatch(ArrowRecordBatch batch)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        ArgumentNullException.ThrowIfNull(batch);

        int colCount = batch.Columns.Count;
        int metaSize = checked(8 + (colCount * ArrowColumnDescriptor.Size));
        byte[]? rented = null;
        Span<byte> metaPayload = metaSize <= 1024
            ? stackalloc byte[metaSize]
            : (rented = ArrayPool<byte>.Shared.Rent(metaSize)).AsSpan(0, metaSize);

        try
        {
            long totalBodyLength = EncodeRecordBatchMetadata(batch, metaPayload);

            // Combined metadata:
            // [MessageType: 1B] [Reserved: 3B] [BodyLength: 8B] [PayloadLength: 4B] [Payload]
            const int metaHeaderLen = 16;
            int totalMetaLen = checked(metaHeaderLen + metaSize);

            Span<byte> frameHeader = stackalloc byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(frameHeader[..4], ArrowIpcParser.ContinuationMarker);
            BinaryPrimitives.WriteInt32LittleEndian(frameHeader.Slice(4, 4), totalMetaLen);
            _stream.Write(frameHeader);

            Span<byte> metaHeader = stackalloc byte[metaHeaderLen];
            metaHeader[0] = (byte)ArrowMessageType.RecordBatch;
            metaHeader[1] = 0;
            metaHeader[2] = 0;
            metaHeader[3] = 0;
            BinaryPrimitives.WriteInt64LittleEndian(metaHeader.Slice(4, 8), totalBodyLength);
            BinaryPrimitives.WriteInt32LittleEndian(metaHeader.Slice(12, 4), metaSize);
            _stream.Write(metaHeader);

            _stream.Write(metaPayload);

            int metaPad = (8 - (totalMetaLen % 8)) % 8;
            Span<byte> zeros = stackalloc byte[8];
            zeros.Clear();
            if (metaPad > 0)
            {
                _stream.Write(zeros[..metaPad]);
            }

            // Stream column buffers directly into destination stream without intermediate heap buffering
            for (int i = 0; i < colCount; i++)
            {
                var col = batch.Columns[i];

                if (col.Field.IsNullable && !col.NullBitmap.IsEmpty)
                {
                    _stream.Write(col.NullBitmap.Span);
                    int pad = (8 - (col.NullBitmap.Length % 8)) % 8;
                    if (pad > 0) _stream.Write(zeros[..pad]);
                }

                if (!col.OffsetsBuffer.IsEmpty)
                {
                    _stream.Write(col.OffsetsBuffer.Span);
                    int pad = (8 - (col.OffsetsBuffer.Length % 8)) % 8;
                    if (pad > 0) _stream.Write(zeros[..pad]);
                }

                if (!col.DataBuffer.IsEmpty)
                {
                    _stream.Write(col.DataBuffer.Span);
                    int pad = (8 - (col.DataBuffer.Length % 8)) % 8;
                    if (pad > 0) _stream.Write(zeros[..pad]);
                }
            }

            _stream.Flush();
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Writes an <see cref="ArrowRecordBatch"/> into <paramref name="writer"/>.
    /// </summary>
    /// <param name="writer">The destination buffer writer.</param>
    /// <param name="batch">The record batch to serialize.</param>
    public static void WriteRecordBatch(IBufferWriter<byte> writer, ArrowRecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(batch);

        int colCount = batch.Columns.Count;
        int metaSize = checked(8 + (colCount * ArrowColumnDescriptor.Size));
        byte[]? rented = null;
        Span<byte> metaPayload = metaSize <= 1024
            ? stackalloc byte[metaSize]
            : (rented = ArrayPool<byte>.Shared.Rent(metaSize)).AsSpan(0, metaSize);

        try
        {
            long totalBodyLength = EncodeRecordBatchMetadata(batch, metaPayload);
            WriteFrameToBufferWriter(writer, ArrowMessageType.RecordBatch, metaPayload, totalBodyLength);

            Span<byte> zeros = stackalloc byte[8];
            zeros.Clear();

            for (int i = 0; i < colCount; i++)
            {
                var col = batch.Columns[i];

                if (col.Field.IsNullable && !col.NullBitmap.IsEmpty)
                {
                    WriteBufferAligned(writer, col.NullBitmap.Span, zeros);
                }

                if (!col.OffsetsBuffer.IsEmpty)
                {
                    WriteBufferAligned(writer, col.OffsetsBuffer.Span, zeros);
                }

                if (!col.DataBuffer.IsEmpty)
                {
                    WriteBufferAligned(writer, col.DataBuffer.Span, zeros);
                }
            }
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    /// Writes the 8-byte Glacier Compact Columnar IPC v1 end-of-stream marker.
    /// </summary>
    public void WriteEndOfStream()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);

        Span<byte> eos = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(eos[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(eos.Slice(4, 4), 0); // 0 indicates EOS
        _stream.Write(eos);
        _stream.Flush();
    }

    /// <summary>
    /// Writes the 8-byte Glacier Compact Columnar IPC v1 end-of-stream marker into <paramref name="writer"/>.
    /// </summary>
    /// <param name="writer">The destination buffer writer.</param>
    public static void WriteEndOfStream(IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        Span<byte> eos = writer.GetSpan(8);
        BinaryPrimitives.WriteUInt32LittleEndian(eos[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(eos.Slice(4, 4), 0);
        writer.Advance(8);
    }

    private static int ComputeSchemaPayloadSize(ArrowSchema schema)
    {
        if (schema.FieldCount > ushort.MaxValue)
        {
            throw new ArgumentException($"Schema field count ({schema.FieldCount}) exceeds maximum of {ushort.MaxValue}.", nameof(schema));
        }

        int total = 2;
        for (int i = 0; i < schema.FieldCount; i++)
        {
            var field = schema.GetField(i);
            int nameBytes = Encoding.UTF8.GetByteCount(field.Name);
            if (nameBytes > byte.MaxValue)
            {
                throw new ArgumentException(
                    $"Field name '{field.Name}' UTF-8 byte length ({nameBytes}) exceeds maximum of {byte.MaxValue} bytes.",
                    nameof(schema));
            }
            total = checked(total + 1 + nameBytes + 1 + 1);
        }
        return total;
    }

    private static void EncodeSchemaPayload(ArrowSchema schema, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(destination[..2], (ushort)schema.FieldCount);
        int offset = 2;
        for (int i = 0; i < schema.FieldCount; i++)
        {
            var field = schema.GetField(i);
            int nameBytes = Encoding.UTF8.GetBytes(field.Name.AsSpan(), destination.Slice(offset + 1));
            destination[offset] = (byte)nameBytes;
            offset += 1 + nameBytes;
            destination[offset++] = (byte)field.DataType.Id;
            destination[offset++] = field.IsNullable ? (byte)1 : (byte)0;
        }
    }

    private static long EncodeRecordBatchMetadata(ArrowRecordBatch batch, Span<byte> metaPayload)
    {
        int colCount = batch.Columns.Count;
        BinaryPrimitives.WriteInt32LittleEndian(metaPayload[..4], batch.RowCount);
        BinaryPrimitives.WriteInt32LittleEndian(metaPayload.Slice(4, 4), colCount);

        long currentBodyOffset = 0;
        for (int i = 0; i < colCount; i++)
        {
            var col = batch.Columns[i];

            int nullOffset = 0;
            int nullLen = 0;
            if (col.Field.IsNullable && !col.NullBitmap.IsEmpty)
            {
                nullOffset = checked((int)currentBodyOffset);
                nullLen = col.NullBitmap.Length;
                currentBodyOffset = checked(currentBodyOffset + nullLen);
                int pad = (8 - (nullLen % 8)) % 8;
                currentBodyOffset = checked(currentBodyOffset + pad);
            }

            int offOffset = 0;
            int offLen = 0;
            if (!col.OffsetsBuffer.IsEmpty)
            {
                offOffset = checked((int)currentBodyOffset);
                offLen = col.OffsetsBuffer.Length;
                currentBodyOffset = checked(currentBodyOffset + offLen);
                int pad = (8 - (offLen % 8)) % 8;
                currentBodyOffset = checked(currentBodyOffset + pad);
            }

            int dataOffset = 0;
            int dataLen = 0;
            if (!col.DataBuffer.IsEmpty)
            {
                dataOffset = checked((int)currentBodyOffset);
                dataLen = col.DataBuffer.Length;
                currentBodyOffset = checked(currentBodyOffset + dataLen);
                int pad = (8 - (dataLen % 8)) % 8;
                currentBodyOffset = checked(currentBodyOffset + pad);
            }

            var desc = new ArrowColumnDescriptor(
                i, col.Length, col.NullCount,
                nullOffset, nullLen,
                offOffset, offLen,
                dataOffset, dataLen);
            desc.WriteTo(metaPayload.Slice(8 + (i * ArrowColumnDescriptor.Size), ArrowColumnDescriptor.Size));
        }

        return currentBodyOffset;
    }

    private void WriteFrame(ArrowMessageType type, ReadOnlySpan<byte> metadataPayload, ReadOnlySpan<byte> body)
    {
        // Combined metadata:
        // [MessageType: 1B] [Reserved: 3B] [BodyLength: 8B] [PayloadLength: 4B] [Payload]
        const int metaHeaderLen = 16;
        int totalMetaLen = checked(metaHeaderLen + metadataPayload.Length);

        Span<byte> frameHeader = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(frameHeader.Slice(4, 4), totalMetaLen);
        _stream.Write(frameHeader);

        Span<byte> metaHeader = stackalloc byte[metaHeaderLen];
        metaHeader[0] = (byte)type;
        metaHeader[1] = 0;
        metaHeader[2] = 0;
        metaHeader[3] = 0;
        BinaryPrimitives.WriteInt64LittleEndian(metaHeader.Slice(4, 8), body.Length);
        BinaryPrimitives.WriteInt32LittleEndian(metaHeader.Slice(12, 4), metadataPayload.Length);
        _stream.Write(metaHeader);

        if (!metadataPayload.IsEmpty)
        {
            _stream.Write(metadataPayload);
        }

        int pad = (8 - (totalMetaLen % 8)) % 8;
        if (pad > 0)
        {
            Span<byte> zeros = stackalloc byte[8];
            zeros.Clear();
            _stream.Write(zeros[..pad]);
        }

        if (!body.IsEmpty)
        {
            _stream.Write(body);
        }

        _stream.Flush();
    }

    private static void WriteFrameToBufferWriter(IBufferWriter<byte> writer, ArrowMessageType type, ReadOnlySpan<byte> metadataPayload, long bodyLength)
    {
        const int metaHeaderLen = 16;
        int totalMetaLen = checked(metaHeaderLen + metadataPayload.Length);
        int pad = (8 - (totalMetaLen % 8)) % 8;
        int headerTotal = checked(8 + metaHeaderLen + metadataPayload.Length + pad);

        Span<byte> dst = writer.GetSpan(headerTotal);
        BinaryPrimitives.WriteUInt32LittleEndian(dst[..4], ArrowIpcParser.ContinuationMarker);
        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(4, 4), totalMetaLen);

        dst[8] = (byte)type;
        dst[9] = 0;
        dst[10] = 0;
        dst[11] = 0;
        BinaryPrimitives.WriteInt64LittleEndian(dst.Slice(12, 8), bodyLength);
        BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(20, 4), metadataPayload.Length);

        if (!metadataPayload.IsEmpty)
        {
            metadataPayload.CopyTo(dst.Slice(24));
        }
        if (pad > 0)
        {
            dst.Slice(24 + metadataPayload.Length, pad).Clear();
        }

        writer.Advance(headerTotal);
    }

    private static void WriteBufferAligned(IBufferWriter<byte> writer, ReadOnlySpan<byte> data, ReadOnlySpan<byte> zeros)
    {
        int pad = (8 - (data.Length % 8)) % 8;
        Span<byte> dst = writer.GetSpan(checked(data.Length + pad));
        data.CopyTo(dst);
        if (pad > 0)
        {
            zeros[..pad].CopyTo(dst.Slice(data.Length));
        }
        writer.Advance(data.Length + pad);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_isDisposed)
        {
            _isDisposed = true;
            if (!_leaveOpen)
            {
                _stream.Dispose();
            }
        }
    }
}
