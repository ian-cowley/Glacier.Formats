namespace Glacier.Formats.Parquet;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

/// <summary>
/// Apache Thrift Compact Protocol wire field types.
/// </summary>
public enum ThriftType : byte
{
    /// <summary>End-of-struct field stop marker.</summary>
    Stop = 0,
    /// <summary>Boolean <c>true</c> encoded inline in the field header.</summary>
    BooleanTrue = 1,
    /// <summary>Boolean <c>false</c> encoded inline in the field header.</summary>
    BooleanFalse = 2,
    /// <summary>Signed 8-bit integer.</summary>
    Byte = 3,
    /// <summary>ZigZag varint 16-bit integer.</summary>
    I16 = 4,
    /// <summary>ZigZag varint 32-bit integer.</summary>
    I32 = 5,
    /// <summary>ZigZag varint 64-bit integer.</summary>
    I64 = 6,
    /// <summary>64-bit little-endian IEEE floating-point number.</summary>
    Double = 7,
    /// <summary>Length-prefixed binary blob or UTF-8 string.</summary>
    Binary = 8,
    /// <summary>Typed list container.</summary>
    List = 9,
    /// <summary>Typed set container.</summary>
    Set = 10,
    /// <summary>Typed key-value map container.</summary>
    Map = 11,
    /// <summary>Nested Thrift struct.</summary>
    Struct = 12
}

[InlineArray(16)]
internal struct ThriftFieldIdStack
{
    private short _element0;
}

/// <summary>
/// Stream-backed Apache Thrift Compact Protocol writer.
/// </summary>
public sealed class ThriftCompactWriter
{
    private const int InlineStackCapacity = 16;
    private const int MaxStructDepth = 256;

    private readonly Stream _stream;
    private ThriftFieldIdStack _fieldIdStack;
    private short[]? _overflowStack;
    private int _structDepth;
    private short _lastFieldId;

    /// <summary>
    /// Initializes a new <see cref="ThriftCompactWriter"/> writing to <paramref name="stream"/>.
    /// </summary>
    /// <param name="stream">The destination writable stream.</param>
    public ThriftCompactWriter(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    }

    /// <summary>Begins writing a Thrift struct (pushes outer delta field ID state and resets to 0).</summary>
    public void WriteStructBegin()
    {
        if (_structDepth >= MaxStructDepth)
        {
            throw new InvalidOperationException($"Thrift struct nesting depth exceeded maximum of {MaxStructDepth}.");
        }

        if (_structDepth < InlineStackCapacity)
        {
            _fieldIdStack[_structDepth] = _lastFieldId;
        }
        else
        {
            _overflowStack ??= new short[MaxStructDepth - InlineStackCapacity];
            _overflowStack[_structDepth - InlineStackCapacity] = _lastFieldId;
        }

        _structDepth++;
        _lastFieldId = 0;
    }

    /// <summary>Ends the current Thrift struct by writing <see cref="ThriftType.Stop"/> and restoring outer delta field ID state.</summary>
    public void WriteStructEnd()
    {
        if (_structDepth <= 0)
        {
            throw new InvalidOperationException("WriteStructEnd called without a matching WriteStructBegin.");
        }

        _stream.WriteByte((byte)ThriftType.Stop);
        _structDepth--;
        _lastFieldId = _structDepth < InlineStackCapacity
            ? _fieldIdStack[_structDepth]
            : _overflowStack![_structDepth - InlineStackCapacity];
    }

    /// <summary>Writes a Thrift field header with delta or full field ID encoding.</summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <param name="type">The Thrift field wire type.</param>
    public void WriteFieldBegin(short fieldId, ThriftType type)
    {
        int delta = fieldId - _lastFieldId;
        if (delta > 0 && delta <= 15)
        {
            byte header = (byte)((delta << 4) | (byte)type);
            _stream.WriteByte(header);
        }
        else
        {
            _stream.WriteByte((byte)type);
            WriteI16(fieldId);
        }
        _lastFieldId = fieldId;
    }

    /// <summary>Writes a Thrift struct <see cref="ThriftType.Stop"/> marker.</summary>
    public void WriteFieldStop()
    {
        _stream.WriteByte((byte)ThriftType.Stop);
    }

    /// <summary>Writes a boolean field whose value is encoded directly in the field header.</summary>
    /// <param name="fieldId">The field identifier.</param>
    /// <param name="value">The boolean value.</param>
    public void WriteBool(short fieldId, bool value)
    {
        var type = value ? ThriftType.BooleanTrue : ThriftType.BooleanFalse;
        WriteFieldBegin(fieldId, type);
    }

    /// <summary>Writes a single byte value.</summary>
    /// <param name="value">The byte value.</param>
    public void WriteByte(byte value)
    {
        _stream.WriteByte(value);
    }

    /// <summary>Writes a ZigZag-encoded 16-bit integer.</summary>
    /// <param name="value">The 16-bit signed integer.</param>
    public void WriteI16(short value)
    {
        WriteVarint32((uint)ZigZagEncode32(value));
    }

    /// <summary>Writes a ZigZag-encoded 32-bit integer.</summary>
    /// <param name="value">The 32-bit signed integer.</param>
    public void WriteI32(int value)
    {
        WriteVarint32((uint)ZigZagEncode32(value));
    }

    /// <summary>Writes a ZigZag-encoded 64-bit integer.</summary>
    /// <param name="value">The 64-bit signed integer.</param>
    public void WriteI64(long value)
    {
        WriteVarint64((ulong)ZigZagEncode64(value));
    }

    /// <summary>Writes a 64-bit little-endian IEEE floating-point number.</summary>
    /// <param name="value">The double-precision value.</param>
    public void WriteDouble(double value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(bytes, value);
        _stream.Write(bytes);
    }

    /// <summary>Writes a varint-length-prefixed binary span.</summary>
    /// <param name="bytes">The binary payload.</param>
    public void WriteBinary(ReadOnlySpan<byte> bytes)
    {
        WriteVarint32((uint)bytes.Length);
        _stream.Write(bytes);
    }

    /// <summary>Writes a UTF-8 length-prefixed string without intermediate heap allocations.</summary>
    /// <param name="value">The string to encode.</param>
    public void WriteString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        int maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (maxBytes <= 256)
        {
            Span<byte> buf = stackalloc byte[256];
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), buf);
            WriteBinary(buf[..written]);
            return;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(maxBytes);
        try
        {
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), rented);
            WriteBinary(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Writes a Thrift list header with element type and element count.</summary>
    /// <param name="elemType">The element wire type.</param>
    /// <param name="size">The non-negative element count.</param>
    public void WriteListBegin(ThriftType elemType, int size)
    {
        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size), size, "List size cannot be negative.");
        }

        if (size <= 14)
        {
            byte header = (byte)((size << 4) | (byte)elemType);
            _stream.WriteByte(header);
        }
        else
        {
            byte header = (byte)(0xF0 | (byte)elemType);
            _stream.WriteByte(header);
            WriteVarint32((uint)size);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ZigZagEncode32(int n) => (n << 1) ^ (n >> 31);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ZigZagEncode64(long n) => (n << 1) ^ (n >> 63);

    private void WriteVarint32(uint n)
    {
        Span<byte> buf = stackalloc byte[5];
        int pos = 0;
        while ((n & ~0x7Fu) != 0)
        {
            buf[pos++] = (byte)((n & 0x7F) | 0x80);
            n >>= 7;
        }
        buf[pos++] = (byte)n;
        _stream.Write(buf[..pos]);
    }

    private void WriteVarint64(ulong n)
    {
        Span<byte> buf = stackalloc byte[10];
        int pos = 0;
        while ((n & ~0x7FUL) != 0)
        {
            buf[pos++] = (byte)((n & 0x7F) | 0x80);
            n >>= 7;
        }
        buf[pos++] = (byte)n;
        _stream.Write(buf[..pos]);
    }
}

/// <summary>
/// Stream-backed Apache Thrift Compact Protocol reader with strict wire-type and varint validation.
/// </summary>
public sealed class ThriftCompactReader
{
    private const int MaxBinaryLength = 64 * 1024 * 1024;
    private const int MaxCollectionLength = 10_000_000;
    private const int MaxSkipDepth = 64;
    private const int InlineStackCapacity = 16;
    private const int MaxStructDepth = 64;

    private readonly Stream _stream;
    private ThriftFieldIdStack _fieldIdStack;
    private short[]? _overflowStack;
    private int _structDepth;
    private short _lastFieldId;

    /// <summary>
    /// Initializes a new <see cref="ThriftCompactReader"/> reading from <paramref name="stream"/>.
    /// </summary>
    /// <param name="stream">The readable source stream.</param>
    public ThriftCompactReader(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    }

    /// <summary>Begins reading a Thrift struct (pushes outer delta field ID state and resets to 0).</summary>
    public void ReadStructBegin()
    {
        if (_structDepth >= MaxStructDepth)
        {
            throw new InvalidDataException($"Thrift struct nesting depth exceeded {MaxStructDepth}.");
        }

        if (_structDepth < InlineStackCapacity)
        {
            _fieldIdStack[_structDepth] = _lastFieldId;
        }
        else
        {
            _overflowStack ??= new short[MaxStructDepth - InlineStackCapacity];
            _overflowStack[_structDepth - InlineStackCapacity] = _lastFieldId;
        }

        _structDepth++;
        _lastFieldId = 0;
    }

    private void PopFieldIdIfStacked()
    {
        if (_structDepth > 0)
        {
            _structDepth--;
            _lastFieldId = _structDepth < InlineStackCapacity
                ? _fieldIdStack[_structDepth]
                : _overflowStack![_structDepth - InlineStackCapacity];
        }
    }

    /// <summary>
    /// Reads the next field header in the current struct.
    /// Returns <c>false</c> when <see cref="ThriftType.Stop"/> is encountered and restores the enclosing struct's field ID state.
    /// Throws <see cref="EndOfStreamException"/> on premature EOF and <see cref="InvalidDataException"/> on unknown wire types.
    /// </summary>
    /// <param name="fieldId">Receives the decoded field ID.</param>
    /// <param name="type">Receives the decoded Thrift wire type.</param>
    /// <returns><c>true</c> if a field header was read; <c>false</c> on struct stop.</returns>
    public bool ReadFieldBegin(out short fieldId, out ThriftType type)
    {
        int b = _stream.ReadByte();
        if (b < 0)
        {
            throw new EndOfStreamException("Unexpected end of Thrift stream while reading field header.");
        }

        if (b == 0)
        {
            fieldId = 0;
            type = ThriftType.Stop;
            PopFieldIdIfStacked();
            return false;
        }

        byte header = (byte)b;
        byte rawType = (byte)(header & 0x0F);
        if (rawType == (byte)ThriftType.Stop)
        {
            fieldId = 0;
            type = ThriftType.Stop;
            PopFieldIdIfStacked();
            return false;
        }

        if (rawType > (byte)ThriftType.Struct)
        {
            throw new InvalidDataException($"Unknown Thrift wire type: {rawType}.");
        }

        type = (ThriftType)rawType;
        short delta = (short)((header >> 4) & 0x0F);
        if (delta != 0)
        {
            fieldId = (short)(_lastFieldId + delta);
        }
        else
        {
            fieldId = ReadI16();
        }

        _lastFieldId = fieldId;
        return true;
    }

    /// <summary>Reads a single byte.</summary>
    /// <returns>The byte read.</returns>
    public byte ReadByte()
    {
        int b = _stream.ReadByte();
        if (b < 0) throw new EndOfStreamException();
        return (byte)b;
    }

    /// <summary>Reads a ZigZag varint 16-bit integer.</summary>
    /// <returns>The decoded 16-bit signed integer.</returns>
    public short ReadI16()
    {
        return (short)ZigZagDecode32((int)ReadVarint32());
    }

    /// <summary>Reads a ZigZag varint 32-bit integer.</summary>
    /// <returns>The decoded 32-bit signed integer.</returns>
    public int ReadI32()
    {
        return ZigZagDecode32((int)ReadVarint32());
    }

    /// <summary>Reads a ZigZag varint 64-bit integer.</summary>
    /// <returns>The decoded 64-bit signed integer.</returns>
    public long ReadI64()
    {
        return ZigZagDecode64((long)ReadVarint64());
    }

    /// <summary>Reads a 64-bit little-endian IEEE floating-point value.</summary>
    /// <returns>The decoded double-precision number.</returns>
    public double ReadDouble()
    {
        Span<byte> bytes = stackalloc byte[8];
        ReadExact(bytes);
        return BinaryPrimitives.ReadDoubleLittleEndian(bytes);
    }

    /// <summary>Reads a varint-length-prefixed binary byte array.</summary>
    /// <returns>The decoded byte array.</returns>
    public byte[] ReadBinary()
    {
        int length = (int)ReadVarint32();
        if (length < 0 || length > MaxBinaryLength)
        {
            throw new InvalidDataException($"Thrift binary length {length} out of valid bounds.");
        }
        var bytes = new byte[length];
        ReadExact(bytes);
        return bytes;
    }

    /// <summary>Reads a varint-length-prefixed UTF-8 string without allocating an intermediate byte array.</summary>
    /// <returns>The decoded string.</returns>
    public string ReadString()
    {
        int length = (int)ReadVarint32();
        if (length < 0 || length > MaxBinaryLength)
        {
            throw new InvalidDataException($"Thrift string length {length} out of valid bounds.");
        }

        if (length == 0) return string.Empty;

        if (length <= 512)
        {
            Span<byte> buf = stackalloc byte[length];
            ReadExact(buf);
            return Encoding.UTF8.GetString(buf);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            Span<byte> span = rented.AsSpan(0, length);
            ReadExact(span);
            return Encoding.UTF8.GetString(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Reads a Thrift list or set header.</summary>
    /// <param name="elemType">Receives the element wire type.</param>
    /// <param name="size">Receives the element count.</param>
    public void ReadListBegin(out ThriftType elemType, out int size)
    {
        byte header = ReadByte();
        byte rawElemType = (byte)(header & 0x0F);
        if (rawElemType == 0 || rawElemType > (byte)ThriftType.Struct)
        {
            throw new InvalidDataException($"Unknown Thrift list element wire type: {rawElemType}.");
        }

        elemType = (ThriftType)rawElemType;
        int s = (header >> 4) & 0x0F;
        if (s == 15)
        {
            size = (int)ReadVarint32();
        }
        else
        {
            size = s;
        }
        if (size < 0 || size > MaxCollectionLength)
        {
            throw new InvalidDataException($"Thrift collection length {size} out of valid bounds.");
        }
    }

    /// <summary>Skips a Thrift value of the specified wire type.</summary>
    /// <param name="type">The Thrift wire type to skip.</param>
    public void Skip(ThriftType type) => Skip(type, depth: 0);

    private void Skip(ThriftType type, int depth)
    {
        if (depth > MaxSkipDepth)
        {
            throw new InvalidDataException($"Thrift nesting depth exceeded {MaxSkipDepth}.");
        }

        switch (type)
        {
            case ThriftType.BooleanTrue:
            case ThriftType.BooleanFalse:
                break;
            case ThriftType.Byte:
                ReadByte();
                break;
            case ThriftType.I16:
                ReadI16();
                break;
            case ThriftType.I32:
                ReadI32();
                break;
            case ThriftType.I64:
                ReadI64();
                break;
            case ThriftType.Double:
                ReadDouble();
                break;
            case ThriftType.Binary:
            {
                int length = (int)ReadVarint32();
                if (length < 0 || length > MaxBinaryLength)
                {
                    throw new InvalidDataException($"Thrift binary length {length} out of valid bounds.");
                }
                if (length > 0)
                {
                    if (_stream.CanSeek)
                    {
                        long remaining = _stream.Length - _stream.Position;
                        if (length > remaining) throw new EndOfStreamException();
                        _stream.Seek(length, SeekOrigin.Current);
                    }
                    else
                    {
                        Span<byte> scratch = stackalloc byte[256];
                        int left = length;
                        while (left > 0)
                        {
                            int toRead = Math.Min(left, scratch.Length);
                            ReadExact(scratch[..toRead]);
                            left -= toRead;
                        }
                    }
                }
                break;
            }
            case ThriftType.List:
            case ThriftType.Set:
                ReadListBegin(out var elemType, out int listSize);
                for (int i = 0; i < listSize; i++) Skip(elemType, depth + 1);
                break;
            case ThriftType.Map:
            {
                int mapSize = (int)ReadVarint32();
                if (mapSize < 0 || mapSize > MaxCollectionLength)
                {
                    throw new InvalidDataException($"Thrift map length {mapSize} out of valid bounds.");
                }
                if (mapSize > 0)
                {
                    byte kvType = ReadByte();
                    byte rawKey = (byte)((kvType >> 4) & 0x0F);
                    byte rawVal = (byte)(kvType & 0x0F);
                    if (rawKey == 0 || rawKey > (byte)ThriftType.Struct ||
                        rawVal == 0 || rawVal > (byte)ThriftType.Struct)
                    {
                        throw new InvalidDataException($"Unknown Thrift map key/value wire type ({rawKey}, {rawVal}).");
                    }

                    var keyType = (ThriftType)rawKey;
                    var valType = (ThriftType)rawVal;
                    for (int i = 0; i < mapSize; i++)
                    {
                        Skip(keyType, depth + 1);
                        Skip(valType, depth + 1);
                    }
                }
                break;
            }
            case ThriftType.Struct:
            {
                ReadStructBegin();
                while (ReadFieldBegin(out _, out var fType)) Skip(fType, depth + 1);
                break;
            }
            default:
                throw new InvalidDataException($"Unknown Thrift wire type: {(byte)type}.");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ZigZagDecode32(int n) => (int)((uint)n >> 1) ^ -(n & 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ZigZagDecode64(long n) => (long)((ulong)n >> 1) ^ -(n & 1);

    private uint ReadVarint32()
    {
        uint result = 0;
        int shift = 0;
        for (int i = 0; i < 5; i++)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new EndOfStreamException();
            if (i == 4 && (b & 0xF0) != 0)
            {
                throw new InvalidDataException("Malformed 32-bit Thrift varint exceeds 5 bytes or overflows UInt32.");
            }
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Malformed 32-bit Thrift varint exceeds 5 bytes.");
    }

    private ulong ReadVarint64()
    {
        ulong result = 0;
        int shift = 0;
        for (int i = 0; i < 10; i++)
        {
            int b = _stream.ReadByte();
            if (b < 0) throw new EndOfStreamException();
            if (i == 9 && (b & 0xFE) != 0)
            {
                throw new InvalidDataException("Malformed 64-bit Thrift varint exceeds 10 bytes or overflows UInt64.");
            }
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Malformed 64-bit Thrift varint exceeds 10 bytes.");
    }

    private void ReadExact(Span<byte> destination)
    {
        int total = 0;
        while (total < destination.Length)
        {
            int r = _stream.Read(destination[total..]);
            if (r == 0) throw new EndOfStreamException();
            total += r;
        }
    }
}

internal sealed class ThriftBufferWriter
{
    private const int InlineStackCapacity = 16;
    private const int MaxStructDepth = 256;

    private readonly IBufferWriter<byte> _writer;
    private ThriftFieldIdStack _fieldIdStack;
    private short[]? _overflowStack;
    private int _structDepth;
    private short _lastFieldId;

    public ThriftBufferWriter(IBufferWriter<byte> writer)
    {
        _writer = writer;
    }

    public void WriteStructBegin()
    {
        if (_structDepth >= MaxStructDepth)
        {
            throw new InvalidOperationException($"Thrift struct nesting depth exceeded maximum of {MaxStructDepth}.");
        }

        if (_structDepth < InlineStackCapacity)
        {
            _fieldIdStack[_structDepth] = _lastFieldId;
        }
        else
        {
            _overflowStack ??= new short[MaxStructDepth - InlineStackCapacity];
            _overflowStack[_structDepth - InlineStackCapacity] = _lastFieldId;
        }

        _structDepth++;
        _lastFieldId = 0;
    }

    public void WriteStructEnd()
    {
        if (_structDepth <= 0)
        {
            throw new InvalidOperationException("WriteStructEnd called without a matching WriteStructBegin.");
        }

        WriteRawByte((byte)ThriftType.Stop);
        _structDepth--;
        _lastFieldId = _structDepth < InlineStackCapacity
            ? _fieldIdStack[_structDepth]
            : _overflowStack![_structDepth - InlineStackCapacity];
    }

    public void WriteFieldBegin(short fieldId, ThriftType type)
    {
        int delta = fieldId - _lastFieldId;
        if (delta > 0 && delta <= 15)
        {
            WriteRawByte((byte)((delta << 4) | (byte)type));
        }
        else
        {
            WriteRawByte((byte)type);
            WriteI16(fieldId);
        }
        _lastFieldId = fieldId;
    }

    public void WriteI32(int value) => WriteVarint32((uint)((value << 1) ^ (value >> 31)));

    public void WriteI64(long value) => WriteVarint64((ulong)((value << 1) ^ (value >> 63)));

    public void WriteI16(short value) => WriteVarint32((uint)(((int)value << 1) ^ ((int)value >> 31)));

    public void WriteString(string value)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (maxBytes <= 256)
        {
            Span<byte> tmp = stackalloc byte[256];
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), tmp);
            WriteVarint32((uint)written);
            if (written > 0)
            {
                Span<byte> dst = _writer.GetSpan(written);
                tmp[..written].CopyTo(dst);
                _writer.Advance(written);
            }
            return;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(maxBytes);
        try
        {
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), rented);
            WriteVarint32((uint)written);
            if (written > 0)
            {
                Span<byte> dst = _writer.GetSpan(written);
                rented.AsSpan(0, written).CopyTo(dst);
                _writer.Advance(written);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public void WriteListBegin(ThriftType elemType, int size)
    {
        if (size < 0) throw new ArgumentOutOfRangeException(nameof(size));
        if (size <= 14)
        {
            WriteRawByte((byte)((size << 4) | (byte)elemType));
        }
        else
        {
            WriteRawByte((byte)(0xF0 | (byte)elemType));
            WriteVarint32((uint)size);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteRawByte(byte b)
    {
        Span<byte> dst = _writer.GetSpan(1);
        dst[0] = b;
        _writer.Advance(1);
    }

    private void WriteVarint32(uint n)
    {
        Span<byte> dst = _writer.GetSpan(5);
        int pos = 0;
        while ((n & ~0x7Fu) != 0)
        {
            dst[pos++] = (byte)((n & 0x7F) | 0x80);
            n >>= 7;
        }
        dst[pos++] = (byte)n;
        _writer.Advance(pos);
    }

    private void WriteVarint64(ulong n)
    {
        Span<byte> dst = _writer.GetSpan(10);
        int pos = 0;
        while ((n & ~0x7FUL) != 0)
        {
            dst[pos++] = (byte)((n & 0x7F) | 0x80);
            n >>= 7;
        }
        dst[pos++] = (byte)n;
        _writer.Advance(pos);
    }
}

internal ref struct ThriftSpanReader
{
    private const int MaxBinaryLength = 64 * 1024 * 1024;
    private const int MaxCollectionLength = 10_000_000;
    private const int MaxSkipDepth = 64;
    private const int InlineStackCapacity = 16;
    private const int MaxStructDepth = 64;

    private readonly ReadOnlySpan<byte> _span;
    private int _offset;
    private ThriftFieldIdStack _fieldIdStack;
    private short[]? _overflowStack;
    private int _structDepth;
    private short _lastFieldId;

    public ThriftSpanReader(ReadOnlySpan<byte> span)
    {
        _span = span;
        _offset = 0;
        _fieldIdStack = default;
        _overflowStack = null;
        _structDepth = 0;
        _lastFieldId = 0;
    }

    public void ReadStructBegin()
    {
        if (_structDepth >= MaxStructDepth)
        {
            throw new InvalidDataException($"Thrift struct nesting depth exceeded {MaxStructDepth}.");
        }

        if (_structDepth < InlineStackCapacity)
        {
            _fieldIdStack[_structDepth] = _lastFieldId;
        }
        else
        {
            _overflowStack ??= new short[MaxStructDepth - InlineStackCapacity];
            _overflowStack[_structDepth - InlineStackCapacity] = _lastFieldId;
        }

        _structDepth++;
        _lastFieldId = 0;
    }

    private void PopFieldIdIfStacked()
    {
        if (_structDepth > 0)
        {
            _structDepth--;
            _lastFieldId = _structDepth < InlineStackCapacity
                ? _fieldIdStack[_structDepth]
                : _overflowStack![_structDepth - InlineStackCapacity];
        }
    }

    public bool ReadFieldBegin(out short fieldId, out ThriftType type)
    {
        byte header = ReadByte();
        if (header == 0)
        {
            fieldId = 0;
            type = ThriftType.Stop;
            PopFieldIdIfStacked();
            return false;
        }

        byte rawType = (byte)(header & 0x0F);
        if (rawType == (byte)ThriftType.Stop)
        {
            fieldId = 0;
            type = ThriftType.Stop;
            PopFieldIdIfStacked();
            return false;
        }

        if (rawType > (byte)ThriftType.Struct)
        {
            throw new InvalidDataException($"Unknown Thrift wire type: {rawType}.");
        }

        type = (ThriftType)rawType;
        short delta = (short)((header >> 4) & 0x0F);
        fieldId = delta != 0 ? (short)(_lastFieldId + delta) : ReadI16();
        _lastFieldId = fieldId;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte ReadByte()
    {
        if (_offset >= _span.Length) throw new EndOfStreamException();
        return _span[_offset++];
    }

    public short ReadI16() => (short)ZigZagDecode32((int)ReadVarint32());
    public int ReadI32() => ZigZagDecode32((int)ReadVarint32());
    public long ReadI64() => ZigZagDecode64((long)ReadVarint64());

    public string ReadString()
    {
        int length = (int)ReadVarint32();
        if (length < 0 || length > MaxBinaryLength)
        {
            throw new InvalidDataException($"Thrift string length {length} out of valid bounds.");
        }
        if ((long)_offset + length > _span.Length) throw new EndOfStreamException();
        string s = length == 0 ? string.Empty : Encoding.UTF8.GetString(_span.Slice(_offset, length));
        _offset += length;
        return s;
    }

    public void ReadListBegin(out ThriftType elemType, out int size)
    {
        byte header = ReadByte();
        byte rawElemType = (byte)(header & 0x0F);
        if (rawElemType == 0 || rawElemType > (byte)ThriftType.Struct)
        {
            throw new InvalidDataException($"Unknown Thrift list element wire type: {rawElemType}.");
        }

        elemType = (ThriftType)rawElemType;
        int s = (header >> 4) & 0x0F;
        size = s == 15 ? (int)ReadVarint32() : s;
        if (size < 0 || size > MaxCollectionLength)
        {
            throw new InvalidDataException($"Thrift collection length {size} out of valid bounds.");
        }
    }

    public void Skip(ThriftType type) => Skip(type, 0);

    private void Skip(ThriftType type, int depth)
    {
        if (depth > MaxSkipDepth) throw new InvalidDataException($"Thrift nesting depth exceeded {MaxSkipDepth}.");

        switch (type)
        {
            case ThriftType.BooleanTrue:
            case ThriftType.BooleanFalse:
                break;
            case ThriftType.Byte:
                ReadByte();
                break;
            case ThriftType.I16:
                ReadI16();
                break;
            case ThriftType.I32:
                ReadI32();
                break;
            case ThriftType.I64:
                ReadI64();
                break;
            case ThriftType.Double:
                if ((long)_offset + 8 > _span.Length) throw new EndOfStreamException();
                _offset += 8;
                break;
            case ThriftType.Binary:
            {
                int length = (int)ReadVarint32();
                if (length < 0 || length > MaxBinaryLength)
                {
                    throw new InvalidDataException($"Thrift binary length {length} out of valid bounds.");
                }
                if ((long)_offset + length > _span.Length) throw new EndOfStreamException();
                _offset += length;
                break;
            }
            case ThriftType.List:
            case ThriftType.Set:
                ReadListBegin(out var elemType, out int listSize);
                for (int i = 0; i < listSize; i++) Skip(elemType, depth + 1);
                break;
            case ThriftType.Map:
            {
                int mapSize = (int)ReadVarint32();
                if (mapSize < 0 || mapSize > MaxCollectionLength)
                {
                    throw new InvalidDataException($"Thrift map length {mapSize} out of valid bounds.");
                }
                if (mapSize > 0)
                {
                    byte kvType = ReadByte();
                    byte rawKey = (byte)((kvType >> 4) & 0x0F);
                    byte rawVal = (byte)(kvType & 0x0F);
                    if (rawKey == 0 || rawKey > (byte)ThriftType.Struct ||
                        rawVal == 0 || rawVal > (byte)ThriftType.Struct)
                    {
                        throw new InvalidDataException($"Unknown Thrift map key/value wire type ({rawKey}, {rawVal}).");
                    }
                    var keyType = (ThriftType)rawKey;
                    var valType = (ThriftType)rawVal;
                    for (int i = 0; i < mapSize; i++)
                    {
                        Skip(keyType, depth + 1);
                        Skip(valType, depth + 1);
                    }
                }
                break;
            }
            case ThriftType.Struct:
            {
                ReadStructBegin();
                while (ReadFieldBegin(out _, out var fType)) Skip(fType, depth + 1);
                break;
            }
            default:
                throw new InvalidDataException($"Unknown Thrift wire type: {(byte)type}.");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ZigZagDecode32(int n) => (int)((uint)n >> 1) ^ -(n & 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ZigZagDecode64(long n) => (long)((ulong)n >> 1) ^ -(n & 1);

    private uint ReadVarint32()
    {
        uint result = 0;
        int shift = 0;
        for (int i = 0; i < 5; i++)
        {
            byte b = ReadByte();
            if (i == 4 && (b & 0xF0) != 0)
            {
                throw new InvalidDataException("Malformed 32-bit Thrift varint exceeds 5 bytes or overflows UInt32.");
            }
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Malformed 32-bit Thrift varint exceeds 5 bytes.");
    }

    private ulong ReadVarint64()
    {
        ulong result = 0;
        int shift = 0;
        for (int i = 0; i < 10; i++)
        {
            byte b = ReadByte();
            if (i == 9 && (b & 0xFE) != 0)
            {
                throw new InvalidDataException("Malformed 64-bit Thrift varint exceeds 10 bytes or overflows UInt64.");
            }
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Malformed 64-bit Thrift varint exceeds 10 bytes.");
    }
}
