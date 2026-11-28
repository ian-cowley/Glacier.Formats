namespace Glacier.Formats.Parquet;

using System;
using System.IO;

/// <summary>
/// Parquet v1 hybrid RLE / bit-packed codec for definition levels, repetition levels, and dictionary indices.
/// </summary>
public static class ParquetRleCodec
{
    /// <summary>
    /// Computes an upper bound on the number of encoded bytes required by <see cref="Encode"/>
    /// for <paramref name="valueCount"/> values of <paramref name="bitWidth"/> bits.
    /// </summary>
    /// <param name="valueCount">The number of values to encode.</param>
    /// <param name="bitWidth">The bit-width of each value (0 to 32).</param>
    /// <returns>A conservative upper bound on the encoded byte length.</returns>
    public static int GetMaxEncodedLength(int valueCount, int bitWidth)
    {
        if (valueCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(valueCount), valueCount, "Value count cannot be negative.");
        }

        if ((uint)bitWidth > 32u)
        {
            throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Bit-width must be between 0 and 32.");
        }

        if (valueCount == 0)
        {
            return 0;
        }

        if (bitWidth == 0)
        {
            return 5;
        }

        int totalGroups = (valueCount + 7) / 8;
        return checked(5 + totalGroups * (1 + bitWidth));
    }

    /// <summary>
    /// Decodes a Parquet hybrid RLE / bit-packed stream of <paramref name="bitWidth"/>-bit values into <paramref name="destination"/>.
    /// </summary>
    /// <param name="source">The encoded Parquet RLE / bit-packed byte span.</param>
    /// <param name="bitWidth">The bit-width of each encoded value (0 to 32).</param>
    /// <param name="destination">The destination span receiving decoded 32-bit unsigned integers.</param>
    /// <returns>The number of decoded values written to <paramref name="destination"/>.</returns>
    public static int Decode(ReadOnlySpan<byte> source, int bitWidth, Span<uint> destination)
    {
        if ((uint)bitWidth > 32u)
        {
            throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Bit-width must be between 0 and 32.");
        }

        if (bitWidth == 0)
        {
            destination.Clear();
            return destination.Length;
        }

        int srcOffset = 0;
        int destOffset = 0;
        int totalDest = destination.Length;

        while (srcOffset < source.Length && destOffset < totalDest)
        {
            uint header = ReadVarint(source, ref srcOffset);
            if ((header & 1) == 0)
            {
                // RLE run: header = (count << 1) | 0
                uint countU = header >> 1;
                int count = countU > (uint)int.MaxValue ? int.MaxValue : (int)countU;
                int valBytes = (bitWidth + 7) >> 3;

                if ((long)srcOffset + valBytes > source.Length)
                {
                    throw new InvalidDataException("Truncated Parquet RLE run value.");
                }

                uint val = 0;
                for (int b = 0; b < valBytes; b++)
                {
                    val |= (uint)source[srcOffset++] << (b * 8);
                }

                int toFill = Math.Min(count, totalDest - destOffset);
                destination.Slice(destOffset, toFill).Fill(val);
                destOffset += toFill;
            }
            else
            {
                // Bit-packed run: header = (numGroups << 1) | 1
                int numGroups = (int)(header >> 1);
                if (numGroups == 0)
                {
                    throw new InvalidDataException("Invalid zero-group Parquet RLE bit-packed header.");
                }

                long count64 = (long)numGroups * 8;
                int byteLen;
                try
                {
                    byteLen = checked(numGroups * bitWidth);
                }
                catch (OverflowException ex)
                {
                    throw new InvalidDataException("Parquet RLE bit-packed run byte length overflows Int32.", ex);
                }

                if ((long)srcOffset + byteLen > source.Length)
                {
                    throw new InvalidDataException("Truncated Parquet RLE bit-packed run.");
                }

                ReadOnlySpan<byte> packed = source.Slice(srcOffset, byteLen);
                srcOffset += byteLen;

                int toUnpack = (int)Math.Min(count64, (long)(totalDest - destOffset));
                VectorizedBitUnpacker.Unpack(packed, bitWidth, destination.Slice(destOffset, toUnpack));
                destOffset += toUnpack;
            }
        }

        return destOffset;
    }

    /// <summary>
    /// Encodes <paramref name="values"/> as a Parquet hybrid RLE / bit-packed stream with <paramref name="bitWidth"/> bits per value.
    /// Emits compact RLE runs for uniform runs of length &gt;= 8 and bit-packed runs otherwise.
    /// </summary>
    /// <param name="values">The unsigned integer values to encode.</param>
    /// <param name="bitWidth">The bit-width of each value (0 to 32).</param>
    /// <param name="destination">The destination byte span.</param>
    /// <returns>The number of encoded bytes written to <paramref name="destination"/>.</returns>
    public static int Encode(ReadOnlySpan<uint> values, int bitWidth, Span<byte> destination)
    {
        if ((uint)bitWidth > 32u)
        {
            throw new ArgumentOutOfRangeException(nameof(bitWidth), bitWidth, "Bit-width must be between 0 and 32.");
        }

        if (values.IsEmpty) return 0;

        int destOffset = 0;

        if (bitWidth == 0)
        {
            WriteVarint(destination, ref destOffset, (uint)values.Length << 1);
            return destOffset;
        }

        uint mask = bitWidth == 32 ? uint.MaxValue : ((1u << bitWidth) - 1u);
        int valBytes = (bitWidth + 7) >> 3;
        int srcOffset = 0;
        int total = values.Length;

        while (srcOffset < total)
        {
            uint firstVal = values[srcOffset] & mask;
            int runLen = 1;
            while (srcOffset + runLen < total && (values[srcOffset + runLen] & mask) == firstVal)
            {
                runLen++;
            }

            if (runLen >= 8)
            {
                uint header = (uint)runLen << 1;
                WriteVarint(destination, ref destOffset, header);

                if ((long)destOffset + valBytes > destination.Length)
                {
                    throw new ArgumentException("Destination buffer is too small.", nameof(destination));
                }

                for (int b = 0; b < valBytes; b++)
                {
                    destination[destOffset++] = (byte)(firstVal >> (b * 8));
                }

                srcOffset += runLen;
                continue;
            }

            // Gather up to 512 values (64 groups of 8) for bit-packing, breaking on group-aligned uniform runs >= 8
            int remaining = total - srcOffset;
            int limit = Math.Min(remaining, 512);
            int packCount = Math.Min(8, limit);

            while (packCount < limit)
            {
                int checkIdx = srcOffset + packCount;
                if (total - checkIdx >= 8)
                {
                    uint v = values[checkIdx] & mask;
                    bool allEqual = true;
                    for (int k = 1; k < 8; k++)
                    {
                        if ((values[checkIdx + k] & mask) != v)
                        {
                            allEqual = false;
                            break;
                        }
                    }

                    if (allEqual) break;
                }

                packCount = Math.Min(packCount + 8, limit);
            }

            int groups = (packCount + 7) >> 3;
            uint bpHeader = ((uint)groups << 1) | 1u;
            WriteVarint(destination, ref destOffset, bpHeader);

            int byteLen = checked(groups * bitWidth);
            if ((long)destOffset + byteLen > destination.Length)
            {
                throw new ArgumentException("Destination buffer is too small.", nameof(destination));
            }

            Span<byte> packDest = destination.Slice(destOffset, byteLen);
            ulong acc = 0;
            int bitsInAcc = 0;
            int outByte = 0;

            for (int i = 0; i < packCount; i++)
            {
                acc |= ((ulong)(values[srcOffset + i] & mask)) << bitsInAcc;
                bitsInAcc += bitWidth;
                while (bitsInAcc >= 8)
                {
                    packDest[outByte++] = (byte)acc;
                    acc >>= 8;
                    bitsInAcc -= 8;
                }
            }

            if (bitsInAcc > 0)
            {
                packDest[outByte++] = (byte)acc;
            }

            if (outByte < byteLen)
            {
                packDest[outByte..].Clear();
            }

            destOffset += byteLen;
            srcOffset += packCount;
        }

        return destOffset;
    }

    private static uint ReadVarint(ReadOnlySpan<byte> span, ref int offset)
    {
        uint res = 0;
        int shift = 0;
        for (int i = 0; i < 5; i++)
        {
            if (offset >= span.Length)
            {
                throw new InvalidDataException("Truncated Parquet RLE varint header.");
            }

            byte b = span[offset++];
            if (i == 4 && (b & 0xF0) != 0)
            {
                throw new InvalidDataException("Malformed Parquet RLE varint header overflows UInt32.");
            }

            res |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return res;
            shift += 7;
        }

        throw new InvalidDataException("Malformed Parquet RLE varint header exceeds 5 bytes.");
    }

    private static void WriteVarint(Span<byte> span, ref int offset, uint val)
    {
        while ((val & ~0x7Fu) != 0)
        {
            if (offset >= span.Length)
            {
                throw new ArgumentException("Destination buffer is too small.", nameof(span));
            }
            span[offset++] = (byte)((val & 0x7F) | 0x80);
            val >>= 7;
        }

        if (offset >= span.Length)
        {
            throw new ArgumentException("Destination buffer is too small.", nameof(span));
        }
        span[offset++] = (byte)val;
    }
}
