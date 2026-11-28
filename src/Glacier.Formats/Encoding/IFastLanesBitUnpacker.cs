namespace Glacier.Formats.Encoding;

using System;

/// <summary>
/// FastLanes SIMD bit-unpacking engine interface.
/// Unpacks 16 32-bit integer lanes at peak hardware throughput.
/// </summary>
public interface IFastLanesBitUnpacker
{
    /// <summary>
    /// Unpacks 16 values of the specified bit-width from the packed buffer into the destination span.
    /// </summary>
    /// <param name="packed">The bit-packed input bytes.</param>
    /// <param name="destination">The destination span receiving at least 16 unpacked 32-bit unsigned integers.</param>
    /// <param name="bitWidth">The bit-width of each packed value (1 to 32).</param>
    void Unpack16Lanes(ReadOnlySpan<byte> packed, Span<uint> destination, int bitWidth);

    /// <summary>
    /// Unpacks 16 values of the specified bit-width starting at a specific lane offset.
    /// </summary>
    /// <param name="packed">The bit-packed input bytes.</param>
    /// <param name="destination">The destination span receiving at least 16 unpacked 32-bit unsigned integers.</param>
    /// <param name="bitWidth">The bit-width of each packed value (1 to 32).</param>
    /// <param name="laneOffset">The zero-based lane offset into the packed stream.</param>
    void Unpack16Lanes(ReadOnlySpan<byte> packed, Span<uint> destination, int bitWidth, int laneOffset);
}
