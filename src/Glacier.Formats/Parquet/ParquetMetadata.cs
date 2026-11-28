namespace Glacier.Formats.Parquet;

using System;
using System.Collections.Generic;

/// <summary>Parquet physical storage types.</summary>
public enum ParquetType
{
    /// <summary>1-bit boolean value.</summary>
    Boolean = 0,
    /// <summary>32-bit signed integer.</summary>
    Int32 = 1,
    /// <summary>64-bit signed integer.</summary>
    Int64 = 2,
    /// <summary>96-bit legacy timestamp integer.</summary>
    Int96 = 3,
    /// <summary>32-bit IEEE floating-point number.</summary>
    Float = 4,
    /// <summary>64-bit IEEE floating-point number.</summary>
    Double = 5,
    /// <summary>Variable-length byte array.</summary>
    ByteArray = 6,
    /// <summary>Fixed-length byte array.</summary>
    FixedLenByteArray = 7
}

/// <summary>Parquet schema field repetition types.</summary>
public enum FieldRepetitionType
{
    /// <summary>Non-nullable required field (definition level 0).</summary>
    Required = 0,
    /// <summary>Nullable optional field (max definition level 1).</summary>
    Optional = 1,
    /// <summary>Repeated nested list field.</summary>
    Repeated = 2
}

/// <summary>Parquet page compression codecs.</summary>
public enum CompressionCodec
{
    /// <summary>Uncompressed page payload.</summary>
    Uncompressed = 0,
    /// <summary>Snappy block compression.</summary>
    Snappy = 1,
    /// <summary>Gzip compression.</summary>
    Gzip = 2,
    /// <summary>LZO compression.</summary>
    Lzo = 3,
    /// <summary>Brotli compression.</summary>
    Brotli = 4,
    /// <summary>LZ4 compression.</summary>
    Lz4 = 5,
    /// <summary>Zstandard compression.</summary>
    Zstd = 6
}

/// <summary>Parquet data and level encodings.</summary>
public enum ParquetEncoding
{
    /// <summary>Plain unencoded values.</summary>
    Plain = 0,
    /// <summary>Legacy plain dictionary encoding.</summary>
    PlainDictionary = 2,
    /// <summary>Hybrid run-length and bit-packed encoding.</summary>
    Rle = 3,
    /// <summary>Bit-packed encoding.</summary>
    BitPacked = 4,
    /// <summary>Delta binary-packed integer encoding.</summary>
    DeltaBinaryPacked = 5,
    /// <summary>Delta length-prefixed byte-array encoding.</summary>
    DeltaLengthByteArray = 6,
    /// <summary>Delta byte-array prefix encoding.</summary>
    DeltaByteArray = 7,
    /// <summary>RLE dictionary index encoding.</summary>
    RleDictionary = 8
}

/// <summary>Parquet page header types.</summary>
public enum PageType
{
    /// <summary>Parquet DataPage v1.</summary>
    DataPage = 0,
    /// <summary>Parquet column index page.</summary>
    IndexPage = 1,
    /// <summary>Parquet dictionary page.</summary>
    DictionaryPage = 2,
    /// <summary>Parquet DataPage v2.</summary>
    DataPageV2 = 3
}

/// <summary>Parquet schema tree node metadata.</summary>
public sealed class SchemaElement
{
    /// <summary>Gets or sets the physical Parquet type for leaf columns.</summary>
    public ParquetType? Type { get; set; }
    /// <summary>Gets or sets the repetition type (Required/Optional/Repeated).</summary>
    public FieldRepetitionType? RepetitionType { get; set; }
    /// <summary>Gets or sets the field or group name.</summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>Gets or sets the number of child elements for group nodes.</summary>
    public int? NumChildren { get; set; }
    /// <summary>Optional Parquet ConvertedType (field 6, e.g., 0 = UTF8).</summary>
    internal int? ConvertedType { get; set; }
}

/// <summary>Metadata for a single Parquet column chunk within a row group.</summary>
public sealed class ColumnMetaData
{
    /// <summary>Gets or sets the physical Parquet type.</summary>
    public ParquetType Type { get; set; }
    /// <summary>Gets or sets the encodings used by pages in this chunk.</summary>
    public List<ParquetEncoding> Encodings { get; set; } = new();
    /// <summary>Gets or sets the column path in the schema.</summary>
    public List<string> PathInSchema { get; set; } = new();
    /// <summary>Gets or sets the compression codec used for pages in this chunk.</summary>
    public CompressionCodec Codec { get; set; }
    /// <summary>Gets or sets the number of logical values in this chunk.</summary>
    public long NumValues { get; set; }
    /// <summary>Gets or sets the total uncompressed size in bytes (headers + payloads).</summary>
    public long TotalUncompressedSize { get; set; }
    /// <summary>Gets or sets the total compressed size in bytes (headers + payloads).</summary>
    public long TotalCompressedSize { get; set; }
    /// <summary>Gets or sets the file offset of the first data page.</summary>
    public long DataPageOffset { get; set; }
    /// <summary>Gets or sets the optional file offset of the dictionary page.</summary>
    public long? DictionaryPageOffset { get; set; }
}

/// <summary>A column chunk entry in a Parquet row group.</summary>
public sealed class ColumnChunk
{
    /// <summary>Gets or sets the file offset of the chunk.</summary>
    public long FileOffset { get; set; }
    /// <summary>Gets or sets the chunk metadata.</summary>
    public ColumnMetaData? MetaData { get; set; }
}

/// <summary>A Parquet row group containing column chunks for a horizontal slice of rows.</summary>
public sealed class RowGroup
{
    /// <summary>Gets or sets the column chunks in this row group.</summary>
    public List<ColumnChunk> Columns { get; set; } = new();
    /// <summary>Gets or sets the total byte size across all column chunks.</summary>
    public long TotalByteSize { get; set; }
    /// <summary>Gets or sets the number of rows in this row group.</summary>
    public long NumRows { get; set; }
}

/// <summary>Top-level Parquet file footer metadata.</summary>
public sealed class FileMetaData
{
    /// <summary>Gets or sets the Parquet format version (typically 1).</summary>
    public int Version { get; set; } = 1;
    /// <summary>Gets or sets the flattened Parquet schema tree.</summary>
    public List<SchemaElement> Schema { get; set; } = new();
    /// <summary>Gets or sets the total row count across all row groups.</summary>
    public long NumRows { get; set; }
    /// <summary>Gets or sets the row groups in the file.</summary>
    public List<RowGroup> RowGroups { get; set; } = new();
    /// <summary>Gets or sets the writer application identifier string.</summary>
    public string CreatedBy { get; set; } = "Glacier.Formats";
}

/// <summary>Parquet DataPage v1 header metadata.</summary>
public sealed class DataPageHeader
{
    /// <summary>Gets or sets the number of logical values (including nulls) in this page.</summary>
    public int NumValues { get; set; }
    /// <summary>Gets or sets the value encoding used by this page.</summary>
    public ParquetEncoding Encoding { get; set; }
    /// <summary>Gets or sets the encoding used for definition levels.</summary>
    public ParquetEncoding DefinitionLevelEncoding { get; set; }
    /// <summary>Gets or sets the encoding used for repetition levels.</summary>
    public ParquetEncoding RepetitionLevelEncoding { get; set; }
}

/// <summary>Parquet DictionaryPage header metadata.</summary>
public sealed class DictionaryPageHeader
{
    /// <summary>Gets or sets the number of entries in the dictionary page.</summary>
    public int NumValues { get; set; }
    /// <summary>Gets or sets the encoding used by the dictionary page.</summary>
    public ParquetEncoding Encoding { get; set; }
}

/// <summary>Header preceding every Parquet page in a column chunk.</summary>
public sealed class PageHeader
{
    /// <summary>Gets or sets the page type.</summary>
    public PageType Type { get; set; }
    /// <summary>Gets or sets the uncompressed page payload byte size (excluding header).</summary>
    public int UncompressedPageSize { get; set; }
    /// <summary>Gets or sets the compressed page payload byte size (excluding header).</summary>
    public int CompressedPageSize { get; set; }
    /// <summary>Gets or sets the DataPage v1 header when <see cref="Type"/> is <see cref="PageType.DataPage"/>.</summary>
    public DataPageHeader? DataPageHeader { get; set; }
    /// <summary>Gets or sets the DictionaryPage header when <see cref="Type"/> is <see cref="PageType.DictionaryPage"/>.</summary>
    public DictionaryPageHeader? DictionaryPageHeader { get; set; }
}
