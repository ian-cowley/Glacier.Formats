namespace Glacier.Formats.Arrow;

using System;
using System.Collections.Generic;

/// <summary>
/// Logical column data type identifiers for Glacier Compact Columnar IPC v1.
/// </summary>
public enum ArrowTypeId : byte
{
    /// <summary>Null-only type.</summary>
    Null = 0,
    /// <summary>1-bit packed boolean.</summary>
    Boolean = 1,
    /// <summary>Signed 8-bit integer.</summary>
    Int8 = 2,
    /// <summary>Unsigned 8-bit integer.</summary>
    UInt8 = 3,
    /// <summary>Signed 16-bit integer.</summary>
    Int16 = 4,
    /// <summary>Unsigned 16-bit integer.</summary>
    UInt16 = 5,
    /// <summary>Signed 32-bit integer.</summary>
    Int32 = 6,
    /// <summary>Unsigned 32-bit integer.</summary>
    UInt32 = 7,
    /// <summary>Signed 64-bit integer.</summary>
    Int64 = 8,
    /// <summary>Unsigned 64-bit integer.</summary>
    UInt64 = 9,
    /// <summary>32-bit IEEE floating-point number.</summary>
    Float = 10,
    /// <summary>64-bit IEEE floating-point number.</summary>
    Double = 11,
    /// <summary>Variable-length UTF-8 encoded string.</summary>
    Utf8 = 12,
    /// <summary>Variable-length binary byte sequence.</summary>
    Binary = 13,
    /// <summary>32-bit days since UNIX epoch.</summary>
    Date32 = 14,
    /// <summary>64-bit milliseconds since UNIX epoch.</summary>
    Date64 = 15,
    /// <summary>64-bit timestamp.</summary>
    Timestamp = 16,
    /// <summary>64-bit elapsed duration.</summary>
    Duration = 17,
    /// <summary>128-bit decimal value.</summary>
    Decimal128 = 18,
    /// <summary>64-bit time of day.</summary>
    Time64 = 19
}

/// <summary>
/// Represents a columnar data type backed by an <see cref="ArrowTypeId"/>.
/// </summary>
/// <param name="Id">The underlying <see cref="ArrowTypeId"/>.</param>
public readonly record struct ArrowType(ArrowTypeId Id)
{
    /// <summary>Null type.</summary>
    public static readonly ArrowType Null = new(ArrowTypeId.Null);
    /// <summary>Boolean type.</summary>
    public static readonly ArrowType Boolean = new(ArrowTypeId.Boolean);
    /// <summary>Signed 8-bit integer type.</summary>
    public static readonly ArrowType Int8 = new(ArrowTypeId.Int8);
    /// <summary>Unsigned 8-bit integer type.</summary>
    public static readonly ArrowType UInt8 = new(ArrowTypeId.UInt8);
    /// <summary>Signed 16-bit integer type.</summary>
    public static readonly ArrowType Int16 = new(ArrowTypeId.Int16);
    /// <summary>Unsigned 16-bit integer type.</summary>
    public static readonly ArrowType UInt16 = new(ArrowTypeId.UInt16);
    /// <summary>Signed 32-bit integer type.</summary>
    public static readonly ArrowType Int32 = new(ArrowTypeId.Int32);
    /// <summary>Unsigned 32-bit integer type.</summary>
    public static readonly ArrowType UInt32 = new(ArrowTypeId.UInt32);
    /// <summary>Signed 64-bit integer type.</summary>
    public static readonly ArrowType Int64 = new(ArrowTypeId.Int64);
    /// <summary>Unsigned 64-bit integer type.</summary>
    public static readonly ArrowType UInt64 = new(ArrowTypeId.UInt64);
    /// <summary>32-bit floating-point type.</summary>
    public static readonly ArrowType Float = new(ArrowTypeId.Float);
    /// <summary>64-bit floating-point type.</summary>
    public static readonly ArrowType Double = new(ArrowTypeId.Double);
    /// <summary>Variable-length UTF-8 string type.</summary>
    public static readonly ArrowType Utf8 = new(ArrowTypeId.Utf8);
    /// <summary>Variable-length binary type.</summary>
    public static readonly ArrowType Binary = new(ArrowTypeId.Binary);
    /// <summary>32-bit date type.</summary>
    public static readonly ArrowType Date32 = new(ArrowTypeId.Date32);
    /// <summary>64-bit date type.</summary>
    public static readonly ArrowType Date64 = new(ArrowTypeId.Date64);
    /// <summary>64-bit timestamp type.</summary>
    public static readonly ArrowType Timestamp = new(ArrowTypeId.Timestamp);
    /// <summary>64-bit duration type.</summary>
    public static readonly ArrowType Duration = new(ArrowTypeId.Duration);
    /// <summary>128-bit decimal type.</summary>
    public static readonly ArrowType Decimal128 = new(ArrowTypeId.Decimal128);
    /// <summary>64-bit time-of-day type.</summary>
    public static readonly ArrowType Time64 = new(ArrowTypeId.Time64);

    /// <summary>
    /// Gets the fixed element byte width for fixed-size types, or <c>0</c> for variable-length/null types.
    /// </summary>
    public int ByteWidth => Id switch
    {
        ArrowTypeId.Boolean or ArrowTypeId.Int8 or ArrowTypeId.UInt8 => 1,
        ArrowTypeId.Int16 or ArrowTypeId.UInt16 => 2,
        ArrowTypeId.Int32 or ArrowTypeId.UInt32 or ArrowTypeId.Float or ArrowTypeId.Date32 => 4,
        ArrowTypeId.Int64 or ArrowTypeId.UInt64 or ArrowTypeId.Double or ArrowTypeId.Date64 or ArrowTypeId.Timestamp or ArrowTypeId.Duration or ArrowTypeId.Time64 => 8,
        ArrowTypeId.Decimal128 => 16,
        _ => 0
    };
}

/// <summary>
/// Describes a named column field in an <see cref="ArrowSchema"/>.
/// </summary>
public sealed class ArrowField
{
    /// <summary>Gets the column name.</summary>
    public string Name { get; }
    /// <summary>Gets the column data type.</summary>
    public ArrowType DataType { get; }
    /// <summary>Gets whether the column may contain null values.</summary>
    public bool IsNullable { get; }

    /// <summary>
    /// Initializes a new <see cref="ArrowField"/>.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="dataType">The column data type.</param>
    /// <param name="isNullable">Whether null values are allowed.</param>
    public ArrowField(string name, ArrowType dataType, bool isNullable = true)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        DataType = dataType;
        IsNullable = isNullable;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Name}: {DataType.Id}{(IsNullable ? "?" : "")}";
}

/// <summary>
/// Ordered collection of <see cref="ArrowField"/> definitions and optional in-memory metadata.
/// </summary>
public sealed class ArrowSchema
{
    /// <summary>Gets the ordered fields in this schema.</summary>
    public IReadOnlyList<ArrowField> Fields { get; }
    /// <summary>Gets optional key-value schema metadata (retained in memory; not serialized in Compact IPC v1).</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; }

    /// <summary>
    /// Initializes a new <see cref="ArrowSchema"/>.
    /// </summary>
    /// <param name="fields">The ordered fields.</param>
    /// <param name="metadata">Optional schema metadata dictionary.</param>
    public ArrowSchema(IEnumerable<ArrowField> fields, IDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(fields);
        Fields = new List<ArrowField>(fields);
        Metadata = metadata != null ? new Dictionary<string, string>(metadata) : new Dictionary<string, string>();
    }

    /// <summary>Gets the number of fields in this schema.</summary>
    public int FieldCount => Fields.Count;

    /// <summary>Gets the field at <paramref name="index"/>.</summary>
    public ArrowField GetField(int index) => Fields[index];

    /// <summary>Gets the field at <paramref name="index"/>.</summary>
    public ArrowField GetFieldByIndex(int index) => Fields[index];

    /// <summary>
    /// Finds the zero-based index of the field matching <paramref name="name"/> (case-insensitive), or <c>-1</c>.
    /// </summary>
    public int GetFieldIndex(string name)
    {
        for (int i = 0; i < Fields.Count; i++)
        {
            if (string.Equals(Fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }
}

/// <summary>
/// Represents a byte-offset and byte-length slice within a record batch body buffer.
/// </summary>
/// <param name="Offset">The starting byte offset.</param>
/// <param name="Length">The byte length.</param>
public readonly record struct ArrowBuffer(long Offset, long Length);

/// <summary>
/// Represents the logical element count and null count of a column node.
/// </summary>
/// <param name="Length">The total number of elements.</param>
/// <param name="NullCount">The number of null elements.</param>
public readonly record struct ArrowFieldNode(long Length, long NullCount);

/// <summary>
/// Message type tags for Glacier Compact Columnar IPC v1 frames.
/// </summary>
public enum ArrowMessageType : byte
{
    /// <summary>No message / end-of-stream marker.</summary>
    None = 0,
    /// <summary>Schema definition message.</summary>
    Schema = 1,
    /// <summary>Dictionary batch message.</summary>
    DictionaryBatch = 2,
    /// <summary>Columnar record batch message.</summary>
    RecordBatch = 3
}
