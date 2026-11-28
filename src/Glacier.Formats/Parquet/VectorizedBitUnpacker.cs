namespace Glacier.Formats.Parquet;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Glacier.Formats.Encoding;

/// <summary>
/// Vectorized bit-unpacking engine using Vector256/Vector512 SIMD, FastLanes odd-width unpacking,
/// and 64-bit unaligned bit-slicing.
/// Unpacks variable bit-width integers (0 to 32 bits) at maximum hardware throughput.
/// </summary>
public static class VectorizedBitUnpacker
{
    /// <summary>
    /// Unpacks <see cref="Span{UInt32}.Length"/> integers of bit-width <paramref name="bitWidth"/> from <paramref name="packed"/>.
    /// </summary>
    /// <param name="packed">The bit-packed source buffer.</param>
    /// <param name="bitWidth">The bit-width of each packed integer (0 to 32).</param>
    /// <param name="destination">The destination span receiving unpacked 32-bit unsigned integers.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Unpack(ReadOnlySpan<byte> packed, int bitWidth, Span<uint> destination)
    {
        if ((uint)bitWidth > 32u)
        {
            throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Bit-width must be between 0 and 32.");
        }

        if (bitWidth == 0)
        {
            destination.Clear();
            return;
        }

        long requiredBytes = ((long)destination.Length * bitWidth + 7) >> 3;
        if (packed.Length < requiredBytes)
        {
            throw new ArgumentException($"Packed buffer is too short. Required: {requiredBytes}, Actual: {packed.Length}", nameof(packed));
        }

        if (bitWidth == 32)
        {
            int byteLen = checked(destination.Length * sizeof(uint));
            MemoryMarshal.Cast<byte, uint>(packed[..byteLen]).CopyTo(destination);
            return;
        }

        if (bitWidth == 1)
        {
            Unpack1Bit(packed, destination);
            return;
        }

        if (bitWidth == 2)
        {
            Unpack2Bit(packed, destination);
            return;
        }

        if (bitWidth == 4)
        {
            Unpack4Bit(packed, destination);
            return;
        }

        if (bitWidth == 8)
        {
            Unpack8Bit(packed, destination);
            return;
        }

        if (bitWidth == 16)
        {
            Unpack16Bit(packed, destination);
            return;
        }

        // Odd / non-power-of-two bit-widths: process 16-lane chunks via FastLanesBitUnpacker, then scalar tail
        int lanes16 = destination.Length & ~15;
        int sliceByteLen = bitWidth * 2; // exact: (16 * bitWidth) / 8
        int byteOffset = 0;
        int i = 0;
        for (; i < lanes16 && (long)byteOffset + sliceByteLen <= packed.Length; i += 16, byteOffset += sliceByteLen)
        {
            FastLanesBitUnpacker.Unpack16(packed.Slice(byteOffset), destination.Slice(i, 16), bitWidth);
        }

        if (i < destination.Length)
        {
            UnpackGeneric(packed.Slice(byteOffset), bitWidth, destination.Slice(i));
        }
    }

    private static readonly Vector256<uint> Bit1Shifts = Vector256.Create(0u, 1u, 2u, 3u, 4u, 5u, 6u, 7u);
    private static readonly Vector256<uint> Bit2Shifts = Vector256.Create(0u, 2u, 4u, 6u, 8u, 10u, 12u, 14u);

    /// <summary>
    /// Unpacks 1-bit packed integers from <paramref name="packed"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="packed">The 1-bit packed source buffer.</param>
    /// <param name="destination">The destination span receiving unpacked values (0 or 1).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Unpack1Bit(ReadOnlySpan<byte> packed, Span<uint> destination)
    {
        int totalValues = destination.Length;
        int i = 0;
        int byteIdx = 0;

        if (Avx2.IsSupported && totalValues >= 8 && packed.Length >= 1)
        {
            var mask1 = Vector256<uint>.One;
            var shifts = Bit1Shifts;
            ref uint dstRef = ref MemoryMarshal.GetReference(destination);
            while (i <= totalValues - 8 && byteIdx < packed.Length)
            {
                var v = Avx2.And(Avx2.ShiftRightLogicalVariable(Vector256.Create((uint)packed[byteIdx++]), shifts), mask1);
                v.StoreUnsafe(ref Unsafe.Add(ref dstRef, i));
                i += 8;
            }
        }

        while (i <= totalValues - 8 && byteIdx < packed.Length)
        {
            byte b = packed[byteIdx++];
            destination[i + 0] = (uint)(b & 1);
            destination[i + 1] = (uint)((b >> 1) & 1);
            destination[i + 2] = (uint)((b >> 2) & 1);
            destination[i + 3] = (uint)((b >> 3) & 1);
            destination[i + 4] = (uint)((b >> 4) & 1);
            destination[i + 5] = (uint)((b >> 5) & 1);
            destination[i + 6] = (uint)((b >> 6) & 1);
            destination[i + 7] = (uint)((b >> 7) & 1);
            i += 8;
        }

        if (i < totalValues && byteIdx < packed.Length)
        {
            byte b = packed[byteIdx];
            int bit = 0;
            while (i < totalValues && bit < 8)
            {
                destination[i++] = (uint)((b >> bit++) & 1);
            }
        }
    }

    /// <summary>
    /// Unpacks 2-bit packed integers from <paramref name="packed"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="packed">The 2-bit packed source buffer.</param>
    /// <param name="destination">The destination span receiving unpacked values (0..3).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Unpack2Bit(ReadOnlySpan<byte> packed, Span<uint> destination)
    {
        int totalValues = destination.Length;
        int i = 0;
        int byteIdx = 0;

        if (Avx2.IsSupported && totalValues >= 8 && packed.Length >= 2)
        {
            var mask2 = Vector256.Create(0x03u);
            var shifts = Bit2Shifts;
            ref uint dstRef = ref MemoryMarshal.GetReference(destination);
            while (i <= totalValues - 8 && byteIdx + 1 < packed.Length)
            {
                uint word = BinaryPrimitives.ReadUInt16LittleEndian(packed.Slice(byteIdx, 2));
                var v = Avx2.And(Avx2.ShiftRightLogicalVariable(Vector256.Create(word), shifts), mask2);
                v.StoreUnsafe(ref Unsafe.Add(ref dstRef, i));
                byteIdx += 2;
                i += 8;
            }
        }

        while (i <= totalValues - 4 && byteIdx < packed.Length)
        {
            byte b = packed[byteIdx++];
            destination[i + 0] = (uint)(b & 0x03);
            destination[i + 1] = (uint)((b >> 2) & 0x03);
            destination[i + 2] = (uint)((b >> 4) & 0x03);
            destination[i + 3] = (uint)((b >> 6) & 0x03);
            i += 4;
        }

        if (i < totalValues && byteIdx < packed.Length)
        {
            byte b = packed[byteIdx];
            int shift = 0;
            while (i < totalValues && shift < 8)
            {
                destination[i++] = (uint)((b >> shift) & 0x03);
                shift += 2;
            }
        }
    }

    /// <summary>
    /// Unpacks 4-bit packed integers from <paramref name="packed"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="packed">The 4-bit packed source buffer.</param>
    /// <param name="destination">The destination span receiving unpacked values (0..15).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void Unpack4Bit(ReadOnlySpan<byte> packed, Span<uint> destination)
    {
        int totalValues = destination.Length;
        int i = 0;
        int byteIdx = 0;

        if (Avx2.IsSupported && totalValues >= 16 && packed.Length >= 8)
        {
            var maskLow = Vector128.Create((byte)0x0F);
            fixed (byte* pSrc = packed)
            fixed (uint* pDst = destination)
            {
                while (i <= totalValues - 16 && byteIdx <= packed.Length - 8)
                {
                    Vector128<byte> packed8 = Vector128.CreateScalarUnsafe(*(ulong*)(pSrc + byteIdx)).AsByte();
                    Vector128<byte> lo = Sse2.And(packed8, maskLow);
                    Vector128<byte> hi = Sse2.And(Sse2.ShiftRightLogical(packed8.AsUInt16(), 4).AsByte(), maskLow);
                    Vector128<byte> interleaved = Sse2.UnpackLow(lo, hi);

                    Vector256<int> w0 = Avx2.ConvertToVector256Int32(interleaved);
                    Vector256<int> w1 = Avx2.ConvertToVector256Int32(Sse2.ShiftRightLogical128BitLane(interleaved, 8));
                    w0.AsUInt32().Store(pDst + i);
                    w1.AsUInt32().Store(pDst + i + 8);
                    byteIdx += 8;
                    i += 16;
                }
            }
        }

        while (i <= totalValues - 2 && byteIdx < packed.Length)
        {
            byte b = packed[byteIdx++];
            destination[i + 0] = (uint)(b & 0x0F);
            destination[i + 1] = (uint)((b >> 4) & 0x0F);
            i += 2;
        }

        if (i < totalValues && byteIdx < packed.Length)
        {
            destination[i] = (uint)(packed[byteIdx] & 0x0F);
        }
    }

    /// <summary>
    /// Unpacks 8-bit packed integers from <paramref name="packed"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="packed">The 8-bit packed source buffer.</param>
    /// <param name="destination">The destination span receiving unpacked values (0..255).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void Unpack8Bit(ReadOnlySpan<byte> packed, Span<uint> destination)
    {
        int count = Math.Min(packed.Length, destination.Length);
        int i = 0;

        if (Avx2.IsSupported && count >= 8)
        {
            fixed (byte* pSrc = packed)
            fixed (uint* pDst = destination)
            {
                int limit = count - 8;
                for (; i <= limit; i += 8)
                {
                    Avx2.ConvertToVector256Int32(pSrc + i).AsUInt32().Store(pDst + i);
                }
            }
        }

        for (; i < count; i++)
        {
            destination[i] = packed[i];
        }
    }

    /// <summary>
    /// Unpacks 16-bit packed integers from <paramref name="packed"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="packed">The 16-bit little-endian packed source buffer.</param>
    /// <param name="destination">The destination span receiving unpacked values (0..65535).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void Unpack16Bit(ReadOnlySpan<byte> packed, Span<uint> destination)
    {
        ReadOnlySpan<ushort> src = MemoryMarshal.Cast<byte, ushort>(packed);
        int count = Math.Min(src.Length, destination.Length);
        int i = 0;

        if (Avx2.IsSupported && count >= 8)
        {
            fixed (ushort* pSrc = src)
            fixed (uint* pDst = destination)
            {
                int limit = count - 8;
                for (; i <= limit; i += 8)
                {
                    Avx2.ConvertToVector256Int32(pSrc + i).AsUInt32().Store(pDst + i);
                }
            }
        }

        for (; i < count; i++)
        {
            destination[i] = src[i];
        }
    }

    /// <summary>
    /// Unpacks integers of arbitrary <paramref name="bitWidth"/> (0 to 32) using 64-bit unaligned scalar bit-slicing.
    /// </summary>
    /// <param name="packed">The bit-packed source buffer.</param>
    /// <param name="bitWidth">The bit-width of each packed integer (0 to 32).</param>
    /// <param name="destination">The destination span receiving unpacked 32-bit unsigned integers.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void UnpackGeneric(ReadOnlySpan<byte> packed, int bitWidth, Span<uint> destination)
    {
        if ((uint)bitWidth > 32u)
        {
            throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Bit-width must be between 0 and 32.");
        }

        if (bitWidth == 0)
        {
            destination.Clear();
            return;
        }

        long requiredBytes = ((long)destination.Length * bitWidth + 7) >> 3;
        if (packed.Length < requiredBytes)
        {
            throw new ArgumentException($"Packed buffer is too short. Required: {requiredBytes}, Actual: {packed.Length}", nameof(packed));
        }

        uint mask = bitWidth == 32 ? uint.MaxValue : ((1u << bitWidth) - 1u);
        int totalValues = destination.Length;
        long bitOffset = 0;

        fixed (byte* pSrc = packed)
        {
            for (int i = 0; i < totalValues; i++)
            {
                int byteOffset = (int)(bitOffset >> 3);
                int bitInByte = (int)(bitOffset & 7);

                // Safe 64-bit load if enough bytes remain
                ulong chunk;
                if (byteOffset + 8 <= packed.Length)
                {
                    chunk = Unsafe.ReadUnaligned<ulong>(pSrc + byteOffset);
                }
                else
                {
                    // Tail read
                    chunk = 0;
                    int bytesLeft = packed.Length - byteOffset;
                    for (int b = 0; b < bytesLeft; b++)
                    {
                        chunk |= (ulong)pSrc[byteOffset + b] << (b * 8);
                    }
                }

                destination[i] = (uint)(chunk >> bitInByte) & mask;
                bitOffset += bitWidth;
            }
        }
    }
}
