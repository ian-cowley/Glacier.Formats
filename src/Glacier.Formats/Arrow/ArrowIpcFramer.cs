namespace Glacier.Formats.Arrow;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Pure C# framing and zero-allocation metadata parser for Glacier Compact Columnar IPC v1
/// (Arrow-inspired columnar buffers with custom 8-byte continuation prefix + 16-byte header + 36-byte descriptor framing;
/// not wire-compatible with Apache Arrow FlatBuffers IPC).
/// </summary>
public static class ArrowIpcParser
{
    /// <summary>32-bit continuation marker (<c>0xFFFFFFFF</c>) preceding message metadata lengths.</summary>
    public const uint ContinuationMarker = 0xFFFFFFFF;

    internal const int MaxMetadataLength = 64 * 1024 * 1024;

    /// <summary>
    /// Attempts to read an 8-byte Glacier Compact Columnar IPC v1 message prefix and extract the non-negative metadata length.
    /// </summary>
    /// <param name="source">The source bytes (must contain at least 8 bytes).</param>
    /// <param name="metadataLength">Receives the decoded non-negative metadata length in bytes.</param>
    /// <returns><c>true</c> if a valid prefix was parsed; otherwise <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadPrefix(ReadOnlySpan<byte> source, out int metadataLength)
    {
        metadataLength = 0;
        if (source.Length < 8) return false;

        uint marker = BinaryPrimitives.ReadUInt32LittleEndian(source[..4]);
        if (marker == ContinuationMarker)
        {
            metadataLength = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(4, 4));
        }
        else
        {
            if (marker > int.MaxValue)
            {
                return false;
            }
            metadataLength = (int)marker;
        }

        if (metadataLength < 0)
        {
            metadataLength = 0;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Attempts to read and validate a Glacier Compact Columnar IPC v1 8-byte prefix and 16-byte message header.
    /// </summary>
    /// <param name="source">The source bytes.</param>
    /// <param name="metadataLength">Receives the raw metadata byte length (<c>0</c> for end-of-stream).</param>
    /// <param name="messageType">Receives the message type.</param>
    /// <param name="bodyLength">Receives the body byte length.</param>
    /// <param name="headerBytesConsumed">Receives the 8-byte-aligned header byte stride (prefix + aligned metadata).</param>
    /// <returns><c>true</c> if a valid header (or end-of-stream marker) was parsed; otherwise <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadMessageHeader(
        ReadOnlySpan<byte> source,
        out int metadataLength,
        out ArrowMessageType messageType,
        out long bodyLength,
        out int headerBytesConsumed)
    {
        messageType = ArrowMessageType.None;
        bodyLength = 0;
        headerBytesConsumed = 0;

        if (!TryReadPrefix(source, out metadataLength))
        {
            return false;
        }

        if (metadataLength == 0)
        {
            headerBytesConsumed = 8;
            return true;
        }

        if (metadataLength < 16 || metadataLength > MaxMetadataLength)
        {
            return false;
        }

        if ((long)source.Length < 8L + metadataLength)
        {
            return false;
        }

        ReadOnlySpan<byte> metaSpan = source.Slice(8, metadataLength);
        byte rawType = metaSpan[0];
        var parsedType = (ArrowMessageType)rawType;
        if (!Enum.IsDefined(parsedType) || parsedType == ArrowMessageType.None)
        {
            return false;
        }

        long parsedBodyLen = BinaryPrimitives.ReadInt64LittleEndian(metaSpan.Slice(4, 8));
        if (parsedBodyLen < 0)
        {
            return false;
        }

        long alignedMeta64 = ((long)metadataLength + 7L) & ~7L;
        if (8L + alignedMeta64 > int.MaxValue)
        {
            return false;
        }

        messageType = parsedType;
        bodyLength = parsedBodyLen;
        headerBytesConsumed = (int)(8L + alignedMeta64);
        return true;
    }
}
