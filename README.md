# Glacier.Formats

Zero-dependency, Native AOT-compatible columnar interchange formats and canonical SIMD primitives for .NET 10.

## Features

- **Glacier Compact Columnar IPC v1 (`Glacier.Formats.Arrow`)**: Arrow-inspired columnar record batches (`ArrowRecordBatch`, `ArrowColumn`, `ArrowSchema`, `ArrowField`) with custom compact binary framing (8-byte continuation prefix + 16-byte message header + 36-byte `ArrowColumnDescriptor` framing). Note: this format is Glacier's proprietary compact columnar IPC wire protocol and is **not** wire-compatible with Apache Arrow FlatBuffers IPC.
- **Parquet v1 Reader & Writer (`Glacier.Formats.Parquet`)**: Pure C# Thrift Compact Protocol metadata serializer/deserializer, `PLAIN` value encoding, RLE/bit-packed definition levels (`FieldRepetitionType.Required` and `FieldRepetitionType.Optional` nullable columns), `Uncompressed` and SIMD-accelerated `SnappyCodec`, and `VectorizedBitUnpacker`.
- **Canonical FastLanes SIMD Bit-Unpacker (`Glacier.Formats.Encoding`)**: `FastLanesBitUnpacker` and `IFastLanesBitUnpacker` supporting 16-lane 32-bit (`1..32`-bit) and 8-lane 64-bit (`1..64`-bit) unpacking with AVX-512, AVX2, and scalar fallbacks.
- **Hardware-Accelerated CRC-32C (`Glacier.Formats.Hashing`)**: `Crc32c` (Castagnoli, polynomial `0x82F63B78`) using x86 SSE4.2, ARM64 CRC32C intrinsics, and lookup-table fallback.

## Installation

```bash
dotnet add package Glacier.Formats
```

## Build & Test

```bash
dotnet build Glacier.Formats.slnx -c Release
dotnet test Glacier.Formats.slnx -c Release
```

## License

MIT
