namespace Glacier.Formats.Parquet;

using System;
using System.Buffers;
using System.IO;

internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;
    private int _index;

    public PooledBufferWriter(int initialCapacity = 256)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 256));
    }

    public int WrittenCount => _index;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);

    public void Reset()
    {
        _index = 0;
    }

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        byte[] buf = _buffer ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));
        if (_index > buf.Length - count)
        {
            throw new InvalidOperationException("Cannot advance past the end of the rented buffer.");
        }
        _index += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_index);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void EnsureCapacity(int sizeHint)
    {
        byte[] buf = _buffer ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));
        int needed = Math.Max(sizeHint, 1);
        if (buf.Length - _index >= needed) return;

        int newSize = Math.Max(buf.Length * 2, checked(_index + needed));
        byte[] next = ArrayPool<byte>.Shared.Rent(newSize);
        buf.AsSpan(0, _index).CopyTo(next);
        ArrayPool<byte>.Shared.Return(buf);
        _buffer = next;
    }

    public void Dispose()
    {
        byte[]? buf = _buffer;
        if (buf != null)
        {
            _buffer = null;
            _index = 0;
            ArrayPool<byte>.Shared.Return(buf);
        }
    }
}

internal sealed class ReadOnlyMemoryStream : Stream
{
    private readonly ReadOnlyMemory<byte> _memory;
    private int _position;

    public ReadOnlyMemoryStream(ReadOnlyMemory<byte> memory)
    {
        _memory = memory;
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _memory.Length;

    public override long Position
    {
        get => _position;
        set
        {
            if (value < 0 || value > _memory.Length)
                throw new ArgumentOutOfRangeException(nameof(value));
            _position = (int)value;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        int available = _memory.Length - _position;
        if (available <= 0 || buffer.IsEmpty) return 0;

        int toCopy = Math.Min(available, buffer.Length);
        _memory.Span.Slice(_position, toCopy).CopyTo(buffer);
        _position += toCopy;
        return toCopy;
    }

    public override int ReadByte()
    {
        if (_position >= _memory.Length) return -1;
        return _memory.Span[_position++];
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        long target = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _memory.Length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (target < 0 || target > int.MaxValue)
            throw new IOException("Attempted to seek outside stream bounds.");

        _position = (int)Math.Min(target, _memory.Length);
        return _position;
    }

    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
