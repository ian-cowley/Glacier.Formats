namespace Glacier.Formats.Arrow;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

/// <summary>
/// Streaming reader for Glacier Compact Columnar IPC v1
/// (Arrow-inspired columnar buffers with custom 8-byte continuation prefix + 16-byte header + 36-byte descriptor framing;
/// not wire-compatible with Apache Arrow FlatBuffers IPC).
/// </summary>
public sealed class ArrowStreamReader : IDisposable
{
    private const int MaxMetadataLength = ArrowIpcParser.MaxMetadataLength;

    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private ArrowSchema? _schema;
    private bool _isDisposed;
    private bool _reachedEos;

    /// <summary>Gets the parsed <see cref="ArrowSchema"/>, or <c>null</c> if not yet read.</summary>
    public ArrowSchema? Schema => _schema;

    /// <summary>Gets whether the stream has not yet reached end-of-stream.</summary>
    public bool HasMoreBatches => !_reachedEos;

    /// <summary>
    /// Initializes a new <see cref="ArrowStreamReader"/> reading from <paramref name="stream"/>.
    /// </summary>
    /// <param name="stream">The readable source stream.</param>
    /// <param name="leaveOpen"><c>true</c> to leave <paramref name="stream"/> open upon disposal.</param>
    public ArrowStreamReader(Stream stream, bool leaveOpen = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _leaveOpen = leaveOpen;
    }

    /// <summary>
    /// Reads and caches the initial <see cref="ArrowSchema"/> message from the stream.
    /// </summary>
    /// <returns>The parsed <see cref="ArrowSchema"/>.</returns>
    public ArrowSchema ReadSchema()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        if (_schema != null) return _schema;

        Span<byte> headerBytes = stackalloc byte[8];
        if (!TryReadPrefixBytes(headerBytes))
        {
            throw new InvalidDataException("Failed to read Glacier IPC schema header prefix.");
        }

        if (!ArrowIpcParser.TryReadPrefix(headerBytes, out int metaLen))
        {
            throw new InvalidDataException("Failed to parse Glacier IPC schema header prefix.");
        }

        if (metaLen == 0)
        {
            _reachedEos = true;
            throw new InvalidDataException("Unexpected end-of-stream before schema message.");
        }

        if (metaLen < 16 || metaLen > MaxMetadataLength)
        {
            throw new InvalidDataException($"Invalid Glacier IPC schema metadata length: {metaLen}.");
        }

        long alignedMeta64 = ((long)metaLen + 7L) & ~7L;
        if (alignedMeta64 > int.MaxValue)
        {
            throw new InvalidDataException($"Aligned schema metadata length overflows Int32: {alignedMeta64}.");
        }

        int alignedMeta = (int)alignedMeta64;
        byte[]? rented = null;
        Span<byte> metaBytes = alignedMeta <= 1024
            ? stackalloc byte[alignedMeta]
            : (rented = ArrayPool<byte>.Shared.Rent(alignedMeta)).AsSpan(0, alignedMeta);

        try
        {
            ReadExactStrict(metaBytes);
            var msgType = (ArrowMessageType)metaBytes[0];
            if (msgType != ArrowMessageType.Schema)
            {
                throw new InvalidDataException($"Expected Glacier IPC Schema message, got {msgType} (0x{metaBytes[0]:X2}).");
            }

            int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(metaBytes.Slice(12, 4));
            if (payloadLen < 0 || 16L + payloadLen > metaLen)
            {
                throw new InvalidDataException($"Invalid Glacier IPC schema payload length {payloadLen} for metadata length {metaLen}.");
            }

            ReadOnlySpan<byte> payload = metaBytes.Slice(16, payloadLen);
            _schema = ParseSchema(payload);
            return _schema;
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
    /// Reads the next <see cref="ArrowRecordBatch"/> from the stream, or returns <c>null</c> at end-of-stream.
    /// </summary>
    /// <returns>The next <see cref="ArrowRecordBatch"/>, or <c>null</c> at EOS.</returns>
    public ArrowRecordBatch? ReadNextRecordBatch()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        if (_reachedEos) return null;

        if (_schema == null)
        {
            ReadSchema();
        }

        Span<byte> headerBytes = stackalloc byte[8];
        if (!TryReadPrefixBytes(headerBytes))
        {
            _reachedEos = true;
            return null;
        }

        if (!ArrowIpcParser.TryReadPrefix(headerBytes, out int metaLen))
        {
            throw new InvalidDataException("Invalid Glacier IPC RecordBatch header prefix.");
        }

        if (metaLen == 0)
        {
            _reachedEos = true;
            return null;
        }

        if (metaLen < 16 || metaLen > MaxMetadataLength)
        {
            throw new InvalidDataException($"Invalid Glacier IPC RecordBatch metadata length: {metaLen}.");
        }

        long alignedMeta64 = ((long)metaLen + 7L) & ~7L;
        if (alignedMeta64 > int.MaxValue)
        {
            throw new InvalidDataException($"Aligned RecordBatch metadata length overflows Int32: {alignedMeta64}.");
        }

        int alignedMeta = (int)alignedMeta64;
        byte[]? rentedMeta = null;
        Span<byte> metaBytes = alignedMeta <= 1024
            ? stackalloc byte[alignedMeta]
            : (rentedMeta = ArrayPool<byte>.Shared.Rent(alignedMeta)).AsSpan(0, alignedMeta);

        try
        {
            ReadExactStrict(metaBytes);

            var msgType = (ArrowMessageType)metaBytes[0];
            if (msgType != ArrowMessageType.RecordBatch)
            {
                throw new InvalidDataException($"Expected Glacier IPC RecordBatch message, got {msgType} (0x{metaBytes[0]:X2}).");
            }

            long bodyLen64 = BinaryPrimitives.ReadInt64LittleEndian(metaBytes.Slice(4, 8));
            int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(metaBytes.Slice(12, 4));
            if (bodyLen64 < 0 || bodyLen64 > Array.MaxLength)
            {
                throw new InvalidDataException($"Invalid Glacier IPC RecordBatch body length: {bodyLen64}.");
            }

            if (payloadLen < 8 || 16L + payloadLen > metaLen)
            {
                throw new InvalidDataException($"Invalid Glacier IPC RecordBatch payload length {payloadLen} for metadata length {metaLen}.");
            }

            ReadOnlySpan<byte> metaPayload = metaBytes.Slice(16, payloadLen);
            int bodyLen = (int)bodyLen64;

            ReadOnlyMemory<byte> bodyMemory;
            if (bodyLen == 0)
            {
                bodyMemory = ReadOnlyMemory<byte>.Empty;
            }
            else if (_stream is MemoryStream ms && ms.CanSeek && ms.TryGetBuffer(out ArraySegment<byte> seg))
            {
                long pos = ms.Position;
                if (pos + bodyLen > ms.Length)
                {
                    throw new EndOfStreamException($"Expected {bodyLen} body bytes but only {ms.Length - pos} remain.");
                }
                bodyMemory = new ReadOnlyMemory<byte>(seg.Array!, seg.Offset + (int)pos, bodyLen);
                ms.Seek(bodyLen, SeekOrigin.Current);
            }
            else
            {
                byte[] bodyBytes = GC.AllocateUninitializedArray<byte>(bodyLen);
                ReadExactStrict(bodyBytes);
                bodyMemory = bodyBytes;
            }

            return BuildRecordBatch(_schema!, metaPayload, bodyMemory);
        }
        finally
        {
            if (rentedMeta != null)
            {
                ArrayPool<byte>.Shared.Return(rentedMeta);
            }
        }
    }

    /// <summary>
    /// Parses a Glacier Compact Columnar IPC v1 <see cref="ArrowSchema"/> message directly from <paramref name="source"/>.
    /// </summary>
    /// <param name="source">The byte span starting at the schema frame prefix.</param>
    /// <param name="bytesConsumed">Receives the total bytes consumed by the schema message.</param>
    /// <returns>The parsed <see cref="ArrowSchema"/>.</returns>
    public static ArrowSchema ReadSchema(ReadOnlySpan<byte> source, out int bytesConsumed)
    {
        if (!ArrowIpcParser.TryReadMessageHeader(source, out int metaLen, out var msgType, out long bodyLen, out int consumed))
        {
            throw new InvalidDataException("Invalid Glacier IPC schema message header.");
        }

        if (metaLen == 0 || msgType != ArrowMessageType.Schema)
        {
            throw new InvalidDataException($"Expected Schema message, got {msgType} (metaLen={metaLen}).");
        }

        if (bodyLen != 0 || source.Length < consumed)
        {
            throw new InvalidDataException("Invalid or truncated Glacier IPC schema frame.");
        }

        ReadOnlySpan<byte> metaSpan = source.Slice(8, metaLen);
        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(metaSpan.Slice(12, 4));
        if (payloadLen < 0 || 16L + payloadLen > metaLen)
        {
            throw new InvalidDataException($"Invalid Glacier IPC schema payload length {payloadLen}.");
        }

        bytesConsumed = consumed;
        return ParseSchema(metaSpan.Slice(16, payloadLen));
    }

    /// <summary>
    /// Zero-copy parses a Glacier Compact Columnar IPC v1 <see cref="ArrowRecordBatch"/> directly from <paramref name="memory"/>.
    /// Returns <c>null</c> on end-of-stream marker.
    /// </summary>
    /// <param name="memory">The memory buffer starting at the record batch frame prefix.</param>
    /// <param name="schema">The active <see cref="ArrowSchema"/>.</param>
    /// <param name="bytesConsumed">Receives the total bytes consumed by the record batch (or EOS marker).</param>
    /// <returns>The zero-copy <see cref="ArrowRecordBatch"/>, or <c>null</c> at EOS.</returns>
    public static ArrowRecordBatch? ReadRecordBatch(ReadOnlyMemory<byte> memory, ArrowSchema schema, out int bytesConsumed)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ReadOnlySpan<byte> span = memory.Span;
        if (!ArrowIpcParser.TryReadMessageHeader(span, out int metaLen, out var msgType, out long bodyLen64, out int headerConsumed))
        {
            throw new InvalidDataException("Invalid Glacier IPC RecordBatch message header.");
        }

        if (metaLen == 0)
        {
            bytesConsumed = headerConsumed;
            return null;
        }

        if (msgType != ArrowMessageType.RecordBatch)
        {
            throw new InvalidDataException($"Expected RecordBatch message, got {msgType}.");
        }

        if (bodyLen64 > Array.MaxLength || (long)headerConsumed + bodyLen64 > span.Length)
        {
            throw new InvalidDataException("Truncated or oversized Glacier IPC RecordBatch body.");
        }

        ReadOnlySpan<byte> metaSpan = span.Slice(8, metaLen);
        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(metaSpan.Slice(12, 4));
        if (payloadLen < 8 || 16L + payloadLen > metaLen)
        {
            throw new InvalidDataException($"Invalid Glacier IPC RecordBatch payload length {payloadLen}.");
        }

        int bodyLen = (int)bodyLen64;
        ReadOnlyMemory<byte> bodyMemory = bodyLen == 0
            ? ReadOnlyMemory<byte>.Empty
            : memory.Slice(headerConsumed, bodyLen);

        bytesConsumed = checked(headerConsumed + bodyLen);
        return BuildRecordBatch(schema, metaSpan.Slice(16, payloadLen), bodyMemory);
    }

    private static ArrowRecordBatch BuildRecordBatch(ArrowSchema schema, ReadOnlySpan<byte> metaPayload, ReadOnlyMemory<byte> bodyMemory)
    {
        if (metaPayload.Length < 8)
        {
            throw new InvalidDataException("Invalid RecordBatch metadata payload.");
        }

        int rowCount = BinaryPrimitives.ReadInt32LittleEndian(metaPayload[..4]);
        int colCount = BinaryPrimitives.ReadInt32LittleEndian(metaPayload.Slice(4, 4));
        if (rowCount < 0 || colCount < 0)
        {
            throw new InvalidDataException($"Invalid RecordBatch dimensions (RowCount={rowCount}, ColumnCount={colCount}).");
        }

        if (colCount != schema.FieldCount)
        {
            throw new InvalidDataException($"RecordBatch ColumnCount ({colCount}) does not match schema FieldCount ({schema.FieldCount}).");
        }

        long requiredMeta = 8L + (long)colCount * ArrowColumnDescriptor.Size;
        if (metaPayload.Length < requiredMeta)
        {
            throw new InvalidDataException($"RecordBatch metadata payload ({metaPayload.Length}) is shorter than {requiredMeta} bytes.");
        }

        var columns = new ArrowColumn[colCount];
        int descOffset = 8;

        int wordCount = (schema.FieldCount + 63) >> 6;
        ulong[]? rentedSeen = null;
        Span<ulong> seenBits = wordCount <= 16
            ? stackalloc ulong[wordCount]
            : (rentedSeen = ArrayPool<ulong>.Shared.Rent(wordCount)).AsSpan(0, wordCount);
        seenBits.Clear();

        try
        {
            for (int i = 0; i < colCount; i++)
            {
                var desc = ArrowColumnDescriptor.ReadFrom(metaPayload.Slice(descOffset, ArrowColumnDescriptor.Size));
                descOffset += ArrowColumnDescriptor.Size;

                if ((uint)desc.FieldIndex >= (uint)schema.FieldCount)
                {
                    throw new InvalidDataException($"Column descriptor FieldIndex {desc.FieldIndex} out of bounds for schema with {schema.FieldCount} fields.");
                }

                int wordIdx = desc.FieldIndex >> 6;
                ulong bitMask = 1UL << (desc.FieldIndex & 63);
                if ((seenBits[wordIdx] & bitMask) != 0)
                {
                    throw new InvalidDataException($"Duplicate column descriptor FieldIndex {desc.FieldIndex} in RecordBatch.");
                }
                seenBits[wordIdx] |= bitMask;

                if (desc.Length != rowCount)
                {
                    throw new InvalidDataException($"Column descriptor length ({desc.Length}) does not match RecordBatch RowCount ({rowCount}).");
                }

                if (desc.NullCount < 0 || desc.NullCount > desc.Length)
                {
                    throw new InvalidDataException($"Invalid column descriptor counts (Length={desc.Length}, NullCount={desc.NullCount}).");
                }

                ValidateBufferSlice("NullBitmap", desc.NullBitmapOffset, desc.NullBitmapLength, bodyMemory.Length);
                ValidateBufferSlice("Offsets", desc.OffsetsOffset, desc.OffsetsLength, bodyMemory.Length);
                ValidateBufferSlice("Data", desc.DataOffset, desc.DataLength, bodyMemory.Length);

                var field = schema.GetField(desc.FieldIndex);

                long requiredNullBytes = (desc.Length + 7L) >> 3;
                if ((desc.NullCount > 0 || desc.NullBitmapLength > 0) && desc.NullBitmapLength < requiredNullBytes)
                {
                    throw new InvalidDataException($"Column '{field.Name}' NullBitmapLength ({desc.NullBitmapLength}) is shorter than required ({requiredNullBytes}) for {desc.Length} rows.");
                }

                if (field.DataType.Id is ArrowTypeId.Utf8 or ArrowTypeId.Binary)
                {
                    long requiredOffsetsBytes = (desc.Length + 1L) * sizeof(int);
                    if ((desc.Length > 0 || desc.OffsetsLength > 0) && desc.OffsetsLength < requiredOffsetsBytes)
                    {
                        throw new InvalidDataException($"Column '{field.Name}' OffsetsLength ({desc.OffsetsLength}) is shorter than required ({requiredOffsetsBytes}) for {desc.Length} rows.");
                    }
                }
                else if (field.DataType.Id == ArrowTypeId.Boolean)
                {
                    long requiredDataBytes = (desc.Length + 7L) >> 3;
                    if ((desc.Length > 0 || desc.DataLength > 0) && desc.DataLength < requiredDataBytes)
                    {
                        throw new InvalidDataException($"Boolean column '{field.Name}' DataLength ({desc.DataLength}) is shorter than required ({requiredDataBytes}) for {desc.Length} rows.");
                    }
                }
                else if (field.DataType.ByteWidth > 0)
                {
                    long requiredDataBytes = (long)desc.Length * field.DataType.ByteWidth;
                    if ((desc.Length > 0 || desc.DataLength > 0) && desc.DataLength < requiredDataBytes)
                    {
                        throw new InvalidDataException($"Column '{field.Name}' DataLength ({desc.DataLength}) is shorter than required ({requiredDataBytes}) for {desc.Length} rows.");
                    }
                }

                ReadOnlyMemory<byte> nullBitmap = desc.NullBitmapLength > 0
                    ? bodyMemory.Slice(desc.NullBitmapOffset, desc.NullBitmapLength)
                    : ReadOnlyMemory<byte>.Empty;

                ReadOnlyMemory<byte> offsets = desc.OffsetsLength > 0
                    ? bodyMemory.Slice(desc.OffsetsOffset, desc.OffsetsLength)
                    : ReadOnlyMemory<byte>.Empty;

                ReadOnlyMemory<byte> data = desc.DataLength > 0
                    ? bodyMemory.Slice(desc.DataOffset, desc.DataLength)
                    : ReadOnlyMemory<byte>.Empty;

                if (field.DataType.Id is ArrowTypeId.Utf8 or ArrowTypeId.Binary && !offsets.IsEmpty)
                {
                    ReadOnlySpan<int> offsetInts = MemoryMarshal.Cast<byte, int>(offsets.Span);
                    int prev = offsetInts[0];
                    if (prev < 0)
                    {
                        throw new InvalidDataException($"Column '{field.Name}' has negative initial offset ({prev}).");
                    }
                    for (int j = 1; j <= desc.Length; j++)
                    {
                        int curr = offsetInts[j];
                        if (curr < prev)
                        {
                            throw new InvalidDataException($"Column '{field.Name}' has non-monotonic offsets at index {j}: {curr} < {prev}.");
                        }
                        prev = curr;
                    }
                    if (prev > desc.DataLength)
                    {
                        throw new InvalidDataException($"Column '{field.Name}' final offset ({prev}) exceeds DataLength ({desc.DataLength}).");
                    }
                }

                columns[i] = new ArrowColumn(field, desc.Length, desc.NullCount, nullBitmap, offsets, data);
            }
        }
        finally
        {
            if (rentedSeen != null)
            {
                ArrayPool<ulong>.Shared.Return(rentedSeen);
            }
        }

        return new ArrowRecordBatch(schema, rowCount, columns);
    }

    private static void ValidateBufferSlice(string bufferName, int offset, int length, int bodyLength)
    {
        if (offset < 0 || length < 0 || (long)offset + length > bodyLength)
        {
            throw new InvalidDataException($"Column {bufferName} slice [{offset}, +{length}] exceeds RecordBatch body length ({bodyLength}).");
        }
    }

    private static ArrowSchema ParseSchema(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2)
        {
            throw new InvalidDataException($"Invalid Glacier IPC schema payload length: {payload.Length}.");
        }

        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(payload[..2]);
        var fields = new List<ArrowField>(count);
        int offset = 2;

        for (int i = 0; i < count; i++)
        {
            if (offset >= payload.Length)
            {
                throw new InvalidDataException($"Truncated Glacier IPC schema payload at field {i} of {count}.");
            }

            byte nameLen = payload[offset++];
            if (offset + nameLen + 2 > payload.Length)
            {
                throw new InvalidDataException($"Truncated Glacier IPC schema field record at field {i} of {count}.");
            }

            string name = Encoding.UTF8.GetString(payload.Slice(offset, nameLen));
            offset += nameLen;

            var typeId = (ArrowTypeId)payload[offset++];
            if (!Enum.IsDefined(typeId))
            {
                throw new InvalidDataException($"Unknown ArrowTypeId {(byte)typeId} in schema field '{name}'.");
            }

            bool isNullable = payload[offset++] != 0;
            fields.Add(new ArrowField(name, new ArrowType(typeId), isNullable));
        }

        return new ArrowSchema(fields);
    }

    private bool TryReadPrefixBytes(Span<byte> buffer)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int r = _stream.Read(buffer[totalRead..]);
            if (r == 0)
            {
                if (totalRead == 0) return false;
                throw new EndOfStreamException($"Expected {buffer.Length} bytes but read {totalRead}.");
            }
            totalRead += r;
        }
        return true;
    }

    private void ReadExactStrict(Span<byte> buffer)
    {
        _stream.ReadExactly(buffer);
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
