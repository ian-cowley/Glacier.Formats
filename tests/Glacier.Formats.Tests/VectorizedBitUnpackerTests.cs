namespace Glacier.Formats.Tests;

using System;
using Glacier.Formats.Parquet;
using Xunit;

public class VectorizedBitUnpackerTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(12)]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    public void Unpack_MatchesScalarReferenceAcrossWidthsAndLengths(int bitWidth)
    {
        int[] lengths = [0, 1, 7, 8, 15, 16, 31, 33, 257];
        ulong mask = bitWidth == 32 ? 0xFFFF_FFFFUL : ((1UL << bitWidth) - 1UL);

        foreach (int len in lengths)
        {
            uint[] expected = new uint[len];
            for (int i = 0; i < len; i++)
            {
                expected[i] = bitWidth == 0 ? 0u : (uint)(((ulong)(i * 2654435761U + 17)) & mask);
            }

            int byteCount = (len * bitWidth + 7) / 8;
            byte[] packed = PackScalar(expected, bitWidth, byteCount);
            uint[] actual = new uint[len];

            VectorizedBitUnpacker.Unpack(packed, bitWidth, actual);
            Assert.Equal(expected, actual);
        }
    }

    private static byte[] PackScalar(uint[] values, int bitWidth, int byteCount)
    {
        byte[] result = new byte[byteCount];
        if (bitWidth == 0) return result;

        int bitOffset = 0;
        for (int i = 0; i < values.Length; i++)
        {
            uint val = values[i];
            for (int b = 0; b < bitWidth; b++)
            {
                if (((val >> b) & 1u) != 0)
                {
                    int idx = bitOffset >> 3;
                    int bit = bitOffset & 7;
                    result[idx] |= (byte)(1 << bit);
                }
                bitOffset++;
            }
        }
        return result;
    }
}
