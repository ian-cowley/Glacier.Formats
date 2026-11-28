namespace Glacier.Formats.Tests;

using System;
using System.Text;
using Glacier.Formats.Hashing;
using Xunit;

public class Crc32cTests
{
    [Fact]
    public void Compute_KnownVector_MatchesCastagnoliStandard()
    {
        byte[] input = "123456789"u8.ToArray();
        uint crc = Crc32c.Compute(input);
        Assert.Equal(0xE3069283u, crc);
    }

    [Fact]
    public void Compute_EmptySpan_ReturnsZero()
    {
        Assert.Equal(0u, Crc32c.Compute(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Update_IncrementalChunks_MatchesSinglePassCompute()
    {
        byte[] data = Encoding.UTF8.GetBytes(new string('G', 137) + "Glacier.Formats.Hashing.Crc32c_Payload_Test!");
        uint expected = Crc32c.Compute(data);

        for (int split = 0; split <= data.Length; split += 7)
        {
            uint state = 0xFFFFFFFFu;
            state = Crc32c.Update(state, data.AsSpan(0, split));
            state = Crc32c.Update(state, data.AsSpan(split));
            uint actual = state ^ 0xFFFFFFFFu;
            Assert.Equal(expected, actual);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(64)]
    [InlineData(1025)]
    public void Compute_VariousLengths_DeterministicAndConsistentWithSoftwareCastagnoli(int length)
    {
        byte[] data = new byte[length];
        new Random(length * 31).NextBytes(data);

        uint actual = Crc32c.Compute(data);
        uint expected = SoftwareCastagnoli(data);
        Assert.Equal(expected, actual);
    }

    private static uint SoftwareCastagnoli(ReadOnlySpan<byte> data)
    {
        const uint poly = 0x82F63B78u;
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data)
        {
            crc ^= b;
            for (int i = 0; i < 8; i++)
            {
                crc = (crc & 1u) != 0 ? (crc >> 1) ^ poly : (crc >> 1);
            }
        }
        return crc ^ 0xFFFFFFFFu;
    }
}
