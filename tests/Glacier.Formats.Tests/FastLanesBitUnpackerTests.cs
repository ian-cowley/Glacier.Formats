namespace Glacier.Formats.Tests;

using System;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Glacier.Formats.Encoding;
using Glacier.Formats.Parquet;
using Xunit;

public class FastLanesBitUnpackerTests
{
    private static void ReferenceOracleUnpack16(ReadOnlySpan<byte> packed, Span<uint> unpacked, int bitWidth)
    {
        uint mask = bitWidth == 32 ? uint.MaxValue : ((1u << bitWidth) - 1u);
        int bitOffset = 0;

        for (int lane = 0; lane < 16 && lane < unpacked.Length; lane++)
        {
            int byteStart = bitOffset >> 3;
            int bitInByte = bitOffset & 7;

            ulong accumulator = 0;
            int bytesLeft = packed.Length - byteStart;
            for (int b = 0; b < 8 && b < bytesLeft; b++)
            {
                accumulator |= ((ulong)packed[byteStart + b]) << (b * 8);
            }

            unpacked[lane] = (uint)((accumulator >> bitInByte) & mask);
            bitOffset += bitWidth;
        }
    }

    private static void ReferenceOracleUnpack8_64(ReadOnlySpan<byte> packed, Span<ulong> unpacked, int bitWidth)
    {
        ulong mask = bitWidth == 64 ? ulong.MaxValue : ((1UL << bitWidth) - 1UL);
        int bitOffset = 0;

        for (int lane = 0; lane < 8 && lane < unpacked.Length; lane++)
        {
            int byteStart = bitOffset >> 3;
            int bitInByte = bitOffset & 7;

            ulong accumulator = 0;
            int bytesLeft = packed.Length - byteStart;
            for (int b = 0; b < 8 && b < bytesLeft; b++)
            {
                accumulator |= ((ulong)packed[byteStart + b]) << (b * 8);
            }

            ulong val = (accumulator >> bitInByte) & mask;
            if (bitInByte + bitWidth > 64 && (byteStart + 8) < packed.Length)
            {
                ulong high = (ulong)packed[byteStart + 8];
                val |= (high << (64 - bitInByte)) & mask;
            }

            unpacked[lane] = val;
            bitOffset += bitWidth;
        }
    }

    [Fact]
    public void Unpack16_ExhaustiveAll32BitWidths_MatchesOracle()
    {
        Span<uint> actual = stackalloc uint[16];
        Span<uint> expected = stackalloc uint[16];

        for (int w = 1; w <= 32; w++)
        {
            int requiredBytes = (16 * w + 7) >> 3;
            byte[] packed = new byte[requiredBytes];
            var rng = new Random(1000 + w);
            rng.NextBytes(packed);

            FastLanesBitUnpacker.Unpack16(packed, actual, w);
            ReferenceOracleUnpack16(packed, expected, w);

            for (int i = 0; i < 16; i++)
            {
                Assert.True(expected[i] == actual[i], $"Mismatch at width {w}, lane {i}: expected {expected[i]}, actual {actual[i]}");
            }
        }
    }

    [Fact]
    public void Unpack8Lanes64_ExhaustiveAll64BitWidths_MatchesOracle()
    {
        Span<ulong> actual = stackalloc ulong[8];
        Span<ulong> expected = stackalloc ulong[8];

        for (int w = 1; w <= 64; w++)
        {
            int requiredBytes = (8 * w + 7) >> 3;
            byte[] packed = new byte[requiredBytes];
            var rng = new Random(2000 + w);
            rng.NextBytes(packed);

            FastLanesBitUnpacker.Unpack8Lanes64(packed, actual, w);
            ReferenceOracleUnpack8_64(packed, expected, w);

            for (int i = 0; i < 8; i++)
            {
                Assert.True(expected[i] == actual[i], $"Mismatch at width {w}, lane {i}: expected {expected[i]}, actual {actual[i]}");
            }
        }
    }

    [Fact]
    public void Unpack16_Vector256_Vector512_And_Scalar_MatchOracle()
    {
        Span<uint> actual = stackalloc uint[16];
        Span<uint> expected = stackalloc uint[16];

        for (int w = 1; w <= 32; w++)
        {
            int requiredBytes = (16 * w + 7) >> 3;
            byte[] packed = new byte[requiredBytes + 64];
            new Random(20000 + w).NextBytes(packed);

            ReferenceOracleUnpack16(packed.AsSpan(0, requiredBytes), expected, w);

            FastLanesBitUnpacker.Unpack16_Scalar(packed.AsSpan(0, requiredBytes), actual, w);
            for (int i = 0; i < 16; i++) Assert.Equal(expected[i], actual[i]);

            if (Vector256.IsHardwareAccelerated && Avx2.IsSupported)
            {
                FastLanesBitUnpacker.Unpack16_Vector256(packed.AsSpan(0, requiredBytes), actual, w, requiredBytes);
                for (int i = 0; i < 16; i++) Assert.Equal(expected[i], actual[i]);
            }

            if (Vector512.IsHardwareAccelerated && Avx512F.IsSupported)
            {
                FastLanesBitUnpacker.Unpack16_Vector512(packed.AsSpan(0, requiredBytes), actual, w, requiredBytes);
                for (int i = 0; i < 16; i++) Assert.Equal(expected[i], actual[i]);
            }
        }
    }

    [Fact]
    public void Unpack8_64_Scalar_ExhaustiveAll64BitWidths_MatchesOracle()
    {
        Span<ulong> actual = stackalloc ulong[8];
        Span<ulong> expected = stackalloc ulong[8];

        for (int w = 1; w <= 64; w++)
        {
            int requiredBytes = (8 * w + 7) >> 3;
            byte[] packed = new byte[requiredBytes];
            new Random(4000 + w).NextBytes(packed);

            FastLanesBitUnpacker.Unpack8_64_Scalar(packed, actual, w);
            ReferenceOracleUnpack8_64(packed, expected, w);

            for (int i = 0; i < 8; i++)
            {
                Assert.Equal(expected[i], actual[i]);
            }
        }
    }

    [Fact]
    public void Unpack16Lanes_ArbitraryLaneOffsets_MatchesOracle()
    {
        IFastLanesBitUnpacker unpacker = FastLanesBitUnpacker.Default;
        Span<uint> actual = stackalloc uint[16];
        Span<uint> expected = stackalloc uint[16];

        int[] testWidths = [1, 2, 3, 4, 5, 7, 8, 9, 11, 13, 16, 17, 24, 31, 32];
        int[] testLaneOffsets = [0, 1, 2, 3, 5, 7, 8, 15, 16, 17, 31, 32, 47, 48];

        foreach (int w in testWidths)
        {
            foreach (int laneOffset in testLaneOffsets)
            {
                int totalBits = (laneOffset + 16) * w;
                int reqBytes = (totalBits + 7) >> 3;
                byte[] buffer = new byte[reqBytes + 16];
                new Random(40000 + w * 100 + laneOffset).NextBytes(buffer);

                unpacker.Unpack16Lanes(buffer.AsSpan(0, reqBytes), actual, w, laneOffset);

                uint mask = w == 32 ? uint.MaxValue : ((1u << w) - 1u);
                int bitOffset = laneOffset * w;
                for (int lane = 0; lane < 16; lane++)
                {
                    int byteStart = bitOffset >> 3;
                    int bitInByte = bitOffset & 7;
                    ulong acc = 0;
                    int bytesLeft = buffer.Length - byteStart;
                    for (int b = 0; b < 8 && b < bytesLeft; b++)
                    {
                        acc |= ((ulong)buffer[byteStart + b]) << (b * 8);
                    }
                    expected[lane] = (uint)((acc >> bitInByte) & mask);
                    bitOffset += w;
                }

                for (int i = 0; i < 16; i++)
                {
                    Assert.Equal(expected[i], actual[i]);
                }
            }
        }
    }

    [Fact]
    public void Unpack1024_ValidAndShortBuffer_BehavesCorrectly()
    {
        for (int w = 1; w <= 32; w += 3)
        {
            int reqBytes = 128 * w;
            byte[] packed = new byte[reqBytes];
            new Random(7000 + w).NextBytes(packed);

            uint[] dest = new uint[1024];
            FastLanesBitUnpacker.Unpack1024(packed, dest, w);

            uint[] expected = new uint[1024];
            VectorizedBitUnpacker.UnpackGeneric(packed, w, expected);
            Assert.Equal(expected, dest);

            Assert.Throws<ArgumentException>(() => FastLanesBitUnpacker.Unpack1024(packed.AsSpan(0, reqBytes - 1), dest, w));
        }
    }

    [Fact]
    public void Validation_InvalidBitWidthsLaneOffsetsAndShortBuffers_Throw()
    {
        byte[] packed = new byte[128];
        uint[] dst16 = new uint[16];
        ulong[] dst8 = new ulong[8];

        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16(packed, dst16, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16(packed, dst16, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16(packed, dst16, 33));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16_Scalar(packed, dst16, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16_Scalar(packed, dst16, 33));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack16_Scalar(packed, dst16, 4, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack8_64_Scalar(packed, dst8, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Unpack8_64_Scalar(packed, dst8, 65));
        Assert.Throws<ArgumentOutOfRangeException>(() => FastLanesBitUnpacker.Default.Unpack16Lanes(packed, dst16, 4, -1));
        Assert.Throws<ArgumentException>(() => FastLanesBitUnpacker.Default.Unpack16Lanes(packed, dst16, 16, int.MaxValue / 2));

        // VectorizedBitUnpacker bounds validation
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorizedBitUnpacker.Unpack(packed, -1, dst16));
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorizedBitUnpacker.Unpack(packed, 33, dst16));
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorizedBitUnpacker.UnpackGeneric(packed, -1, dst16));
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorizedBitUnpacker.UnpackGeneric(packed, 33, dst16));
        Assert.Throws<ArgumentException>(() => VectorizedBitUnpacker.Unpack(packed.AsSpan(0, 2), 5, dst16));
    }

    [Fact]
    public void VectorizedBitUnpacker_ExhaustiveAllWidths0To32_MatchesScalarOracle()
    {
        int[] lengths = [0, 1, 7, 15, 16, 17, 31, 32, 33, 64, 100];
        for (int w = 0; w <= 32; w++)
        {
            uint mask = w == 32 ? uint.MaxValue : w == 0 ? 0u : ((1u << w) - 1u);
            foreach (int len in lengths)
            {
                uint[] expected = new uint[len];
                for (int i = 0; i < len; i++)
                {
                    expected[i] = w == 0 ? 0u : (uint)((i * 2654435761U + 17) & mask);
                }

                int byteCount = (len * w + 7) >> 3;
                byte[] packed = new byte[byteCount];
                int bitOffset = 0;
                for (int i = 0; i < len; i++)
                {
                    for (int b = 0; b < w; b++)
                    {
                        if (((expected[i] >> b) & 1u) != 0)
                        {
                            packed[(bitOffset + b) >> 3] |= (byte)(1 << ((bitOffset + b) & 7));
                        }
                    }
                    bitOffset += w;
                }

                uint[] actual = new uint[len];
                VectorizedBitUnpacker.Unpack(packed, w, actual);
                Assert.Equal(expected, actual);
            }
        }
    }
}
